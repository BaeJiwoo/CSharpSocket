using ServerProgram;

class Program
{
    static void Main()
    {
        SimpleTCPServer tcpServer = new SimpleTCPServer();

        // 1. 서버 초기화 및 시작
        tcpServer.Init(5000);
        tcpServer.BindAndListenAny();
        tcpServer.StartServer(100, 8);

        Console.WriteLine("======================================");
        Console.WriteLine("[System] 서버가 가동되었습니다.");
        Console.WriteLine("[System] 종료하려면 'quit'을 입력하세요.");
        Console.WriteLine("======================================");

        // 2. 명령 입력 루프
        while (true)
        {
            Console.Write("> ");
            string? serverCmd = Console.ReadLine();

            if (string.IsNullOrEmpty(serverCmd)) continue;

            // 소문자로 변환하여 비교 (사용자 편의성)
            if (serverCmd.ToLower() == "quit")
            {
                Console.WriteLine("[System] 서버 종료 프로세스를 시작합니다...");
                break;
            }
            else
            {
                Console.WriteLine($"[System] '{serverCmd}'은(는) 알 수 없는 명령어입니다.");
            }
        }

        // 3. 서버 리소스 정리 및 쓰레드 종료 대기
        tcpServer.EndServer();

        Console.WriteLine("[System] 서버가 완전히 종료되었습니다. 프로그램을 닫습니다.");
    }
}