public enum NetResult
{
    Success = 0,
    SocketError,
    PortAlreadyInUse,  // 포트 중복
    AccessDenied,      // 권한 문제 (보통 1024번 이하 포트 사용 시)
    UnknownError
}