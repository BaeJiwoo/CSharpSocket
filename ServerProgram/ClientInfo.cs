using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ServerProgram
{
    public class ClientInfo
    {
        public ClientInfo()
        {
            mClientSocket = null;
        }
        public void InitSocket(Socket socket)
        {
            mClientSocket = socket;
        }
        public void SetClientIndex(UInt64 clientIndex)
        {
            mClientIndex = clientIndex;
        }
        public UInt64? GetClientIndex() {return mClientIndex;}
        public bool IsConnected()
        {
            if (mClientSocket == null) return false;

            try
            {
                return !(mClientSocket.Poll(1, SelectMode.SelectRead) && mClientSocket.Available == 0);
            }
            catch (SocketException)
            {
                return false;
            }
        }

        public void BindRecvAsyncEventArgs(SocketAsyncEventArgs socketAsyncEvent)
        {
            mRecvArgs = socketAsyncEvent;

            mRecvArgs.UserToken = this;

        }

        public void BindSendAsyncEventArgs(SocketAsyncEventArgs socketAsyncEvent)
        {
            mSendArgs = socketAsyncEvent;

            mSendArgs.UserToken = this;

        }

        public void BindRecv()
        {
            if (mClientSocket == null || mRecvArgs == null) return;

            // [추가] 이미 수신 대기 중이라면 중복 호출 방지
            if (Interlocked.CompareExchange(ref mReceivingValue, 1, 0) == 0)
            {
                try
                {
                    bool pending = mClientSocket.ReceiveAsync(mRecvArgs);
                    if (!pending)
                    {
                        // 즉시 완료 시 수동으로 후속 처리 호출 로직이 필요할 수 있음
                        // 보통은 OnIOCompleted가 알아서 호출되도록 설정됨
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Error] ReceiveAsync failed: {ex.Message}");
                    Interlocked.Exchange(ref mReceivingValue, 0);
                }
            }
        }

        // 수신 처리가 완전히 끝났을 때 호출하여 플래그를 해제해야 함
        public void ReceiveCompleted()
        {
            Interlocked.Exchange(ref mReceivingValue, 0);
        }

        public void Send(byte[] data)
        {
            // [개선] 데이터 오염 방지를 위해 복사본을 큐에 삽입 (선택 사항)
            byte[] copyData = new byte[data.Length];
            Array.Copy(data, copyData, data.Length);

            mSendQueue.Enqueue(copyData);

            if (Interlocked.CompareExchange(ref mSendingValue, 1, 0) == 0)
            {
                StartSend();
            }
        }

        private void StartSend()
        {
            if (mSendQueue.TryDequeue(out byte[]? data))
            {
                mSendArgs!.SetBuffer(data, 0, data.Length);

                try
                {
                    if (!mClientSocket!.SendAsync(mSendArgs))
                    {
                        SendNext();
                    }
                }
                catch (ObjectDisposedException) { /* 소켓 닫힘 처리 */ }
            }
            else
            {
                Interlocked.Exchange(ref mSendingValue, 0);
                if (!mSendQueue.IsEmpty && Interlocked.CompareExchange(ref mSendingValue, 1, 0) == 0)
                {
                    StartSend();
                }
            }
        }

        public void SendNext()
        {
            // 송신이 하나 완료되었으므로 다음 데이터가 있다면 이어서 보냄
            StartSend();
        }

        public void Close()
        {
            if (mClientSocket != null)
            {
                mClientSocket.Close();
                mClientSocket = null;
            }
        }

        public bool IsActive => IsConnected();

        private Socket? mClientSocket = null;
        private UInt64? mClientIndex = null;
        private SocketAsyncEventArgs? mRecvArgs = null;
        private SocketAsyncEventArgs? mSendArgs = null;
        private byte[] mBuffer = new byte[1024];
        private ConcurrentQueue<byte[]> mSendQueue = new ConcurrentQueue<byte[]>();
        private int mSendingValue = 0;
        private int mReceivingValue = 0;
    }
}
