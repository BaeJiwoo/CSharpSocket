using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace ClientProgram
{
    class SimpleTCPClient
    {
        private Socket _clientSocket;
        private byte[] _receiveBuffer = new byte[1024];

        public void Start(string ip, int port)
        {
            try
            {
                _clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                // 서버 연결
                _clientSocket.BeginConnect(new IPEndPoint(IPAddress.Parse(ip), port), ConnectCallback, _clientSocket);

                Console.WriteLine($"[System] Connecting to {ip}:{port}...");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Error] {ex.Message}");
            }
        }

        private void ConnectCallback(IAsyncResult ar)
        {
            try
            {
                Socket client = (Socket)ar.AsyncState;
                client.EndConnect(ar);
                Console.WriteLine("[System] Connected to Server!");

                // 데이터 수신 대기 시작
                Receive();

                // 메시지 입력 루프 시작
                SendLoop();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Error] Connect failed: {ex.Message}");
            }
        }

        private void Receive()
        {
            try
            {
                _clientSocket.BeginReceive(_receiveBuffer, 0, _receiveBuffer.Length, SocketFlags.None, ReceiveCallback, null);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[System] Disconnected: {ex.Message}");
            }
        }

        private void ReceiveCallback(IAsyncResult ar)
        {
            try
            {
                int bytesRead = _clientSocket.EndReceive(ar);
                if (bytesRead > 0)
                {
                    string message = Encoding.UTF8.GetString(_receiveBuffer, 0, bytesRead);
                    Console.WriteLine($"[Server Echo] {message}");
                    Console.Write("> "); // 입력 프롬프트 유지

                    // 다시 수신 대기
                    Receive();
                }
                else
                {
                    Console.WriteLine("[System] Server closed connection.");
                    Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[System] Receive Error: {ex.Message}");
                Close();
            }
        }

        private void SendLoop()
        {
            while (_clientSocket != null && _clientSocket.Connected)
            {
                Console.Write("> ");
                string msg = Console.ReadLine();
                if (string.IsNullOrEmpty(msg)) continue;
                if (msg == "exit") break;

                byte[] data = Encoding.UTF8.GetBytes(msg);
                _clientSocket.Send(data); // 간단한 테스트를 위해 동기 Send 사용
            }
            Close();
        }

        private void Close()
        {
            _clientSocket?.Close();
            _clientSocket = null;
            Environment.Exit(0);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            SimpleTCPClient client = new SimpleTCPClient();
            // 서버 포트 설정 (서버 코드의 mPort와 일치시켜주세요)
            client.Start("127.0.0.1", 5000);

            // 프로그램 종료 방지
            while (true) { Thread.Sleep(1000); }
        }
    }
}