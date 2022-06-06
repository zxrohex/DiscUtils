using System;

namespace DiscUtils.Nfs;

internal interface IRpcClient : IDisposable
{
    RpcCredentials Credentials { get; }
    IRpcTransport GetTransport(int program, int version);
    uint NextTransactionId();
}
