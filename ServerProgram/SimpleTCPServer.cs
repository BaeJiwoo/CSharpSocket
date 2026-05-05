using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Collections.Concurrent;

namespace ServerProgram
{
    public class SimpleTCPServer
    {
        // public Methods
        public SimpleTCPServer() { }
        public void Init(UInt16 port)
        {
            this.mPort = port;
        }

        public NetResult BindAndListenAny()
        {
            try
            {
                mListener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                mListener.Bind(new IPEndPoint(IPAddress.Any, mPort));
                mListener.Listen(5);

                return NetResult.Success;
            }
            catch (SocketException ex)
            {
                // 리소스 정리 (C++의 cleanup 역할)
                CloseListener();

                // 에러 코드별로 분기하여 Enum 반환
                return ex.SocketErrorCode switch
                {
                    SocketError.AddressAlreadyInUse => NetResult.PortAlreadyInUse,
                    SocketError.AccessDenied => NetResult.AccessDenied,
                    _ => NetResult.SocketError
                };  
            }
            catch (Exception)
            {
                CloseListener();
                return NetResult.UnknownError;
            }
        }

            

        public void StartServer(UInt32 clientCount, UInt16 threadCount)
        {
            //CreateClient
            CreateClient(clientCount);

            //AcceptThread
            //mAcceptRunning = true;
            //CreateAcceptThread();

            mAcceptRunning = true;
            mAcceptArgs = new SocketAsyncEventArgs();
            mAcceptArgs.Completed += OnAcceptCompleted;
            RegisterAccept(mAcceptArgs);

            //WorkerThread
            mWorkerRunning = true;
            CreateWorkerThread(threadCount);

            Console.WriteLine("[System] Server Started...");
        }


        public void EndServer()
        {
            // TODO: 생성된 쓰레드 모두 join후 리스너 Close
            mWorkerRunning = false;
            foreach (var worker in mWorkerThreads) {
                if (worker.IsAlive)
                {
                    worker.Join();
                }
            }
            Console.WriteLine("[System] Worker Thread Destoryed");

        }

        // private Methods
        private void CreateClient(UInt32 clientCount)
        {
            for (int i = 0; i < clientCount; i++)
            {
                ClientInfo client = new ClientInfo();
                SocketAsyncEventArgs recvArgs = new SocketAsyncEventArgs();
                recvArgs.SetBuffer(new byte[1024], 0, 1024);
                recvArgs.Completed += OnIOCompleted;
                client.BindRecvAsyncEventArgs(recvArgs);

                SocketAsyncEventArgs sendArgs = new SocketAsyncEventArgs();
                sendArgs.Completed += OnIOCompleted;
                client.BindSendAsyncEventArgs(sendArgs);

                mClientCnt++;
                client.SetClientIndex(mClientCnt);
                mClientList.Add(client);
            }
        }


        private void RegisterAccept(SocketAsyncEventArgs args)
        {
            args.AcceptSocket = null;
            if (mListener != null && !mListener.AcceptAsync(args)) OnAcceptCompleted(null, args);
        }

        private void OnAcceptCompleted(object? sender, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success)
            {
                Socket? newSocket = args.AcceptSocket;
                if (newSocket != null)
                {
                    lock (mClientList)
                    {
                        foreach (var client in mClientList)
                        {
                            if (!client.IsActive)
                            {
                                client.InitSocket(newSocket);
                                 
                                client.BindRecv();
                                break;
                            }
                        }
                    }
                }
            }
            if (mAcceptRunning) RegisterAccept(args);
        }

        //private void CreateAcceptThread()
        //{
        //    mAcceptThread = new Thread(() => { AcceptThread(); });
        //}

        // 아마 이렇게 하면 Accept()가 블로킹이라 서버 절대 안 끝날 거 같음
        // 비동기로 바꿔야 할 듯
        //private void AcceptThread()
        //{
        //    // Accept 시 비어있는 유저에게 데이터 넣고 처리
        //    while (mAcceptRunning)
        //    {
        //        Socket newSocket = mListener.Accept();
        //        foreach (var client in mClientList)
        //        {
        //            if (!client.IsActive)
        //            {
        //                client.InitSocket(newSocket);
        //                break;
        //            }
        //        }
        //        Thread.Sleep(10);
        //    }
        //}


        private void CreateWorkerThread(UInt16 threadCount)
        {
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() => { WorkerThread(); });
                mWorkerThreads.Add(t);
                t.Start();

                Console.WriteLine($"[System] Thread Created: {t.ManagedThreadId}");
            }
        }

        private void OnIOCompleted(object? sender, SocketAsyncEventArgs e)
        {
            // C++의 GQCS에서 어떤 Overlapped 작업이 끝났는지 확인하는 것과 같음
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    mJobQueue.Enqueue(e); // 기존대로 워커 쓰레드에게 넘김
                    mWorkerSignal.Set();


                    break;

                case SocketAsyncOperation.Send:
                    // 송신 완료 시에는 별도의 로직보다 다음 데이터가 있는지 확인해서 보냄
                    if (e.UserToken is ClientInfo client)
                    {
                        client.SendNext();
                    }
                    break;
            }
        }

        private void WorkerThread()
        {
            while (mWorkerRunning)
            {
                // 큐에 일감이 생길 때까지 대기 (GQCS의 블로킹 대기와 유사)
                mWorkerSignal.WaitOne(100);

                while (mJobQueue.TryDequeue(out SocketAsyncEventArgs? e))
                {
                    if (e.UserToken is ClientInfo client)
                    {
                        ProcessPacket(client, e);
                        //client.Send(e.Buffer);
                        client.ReceiveCompleted();
                        // 처리가 끝났으니 다시 예약 (WSARecv)
                        client.BindRecv();
                    }
                }
            }
        }

        private void ProcessPacket(ClientInfo client, SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success && e.BytesTransferred > 0)
            {
                // 1. 데이터를 별도 공간에 복사 (송신과 수신 버퍼 분리)
                byte[] receivedData = new byte[e.BytesTransferred];
                Array.Copy(e.Buffer!, e.Offset, receivedData, 0, e.BytesTransferred);

                Console.WriteLine($"[Client {client.GetClientIndex()}] {e.BytesTransferred} bytes received.");

                // 2. 복사된 데이터를 보냄 (e.Buffer를 직접 넣지 마세요!)
                client.Send(receivedData);
            }
            else
            {
                Console.WriteLine($"[System] Client {client.GetClientIndex()} Disconnected.");
                client.Close();
            }
        }

        private void CloseListener()
        {
            if (mListener != null)
            {
                mListener.Close();
                mListener = null;
            }
        }

        // properties
        private Socket? mListener;

        UInt16 mPort;


        List<Thread> mWorkerThreads = new List<Thread>();

        bool mWorkerRunning = false;

        bool mAcceptRunning = false;

        List<ClientInfo> mClientList = new List<ClientInfo>();

        private SocketAsyncEventArgs? mAcceptArgs;

        private SocketAsyncEventArgs? mRecvArgs;

        private UInt64 mClientCnt = 0;

        private ConcurrentQueue<SocketAsyncEventArgs> mJobQueue = new ConcurrentQueue<SocketAsyncEventArgs>();

        private AutoResetEvent mWorkerSignal = new AutoResetEvent(false);
    }

}
