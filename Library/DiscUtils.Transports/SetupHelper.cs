using DiscUtils.Iscsi;
using DiscUtils.Nfs;
using DiscUtils.OpticalDisk;

namespace DiscUtils.Transports;

public static class SetupHelper
{
    public static void SetupTransports()
    {
        Setup.SetupHelper.RegisterAssembly(typeof(Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(NfsFileSystem).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Disc).Assembly);
    }
}