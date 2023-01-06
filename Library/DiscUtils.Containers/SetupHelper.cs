using DiscUtils.Dmg;
using DiscUtils.Iso9660;
using DiscUtils.Wim;

namespace DiscUtils.Containers;

public static class SetupHelper
{
    public static void SetupContainers()
    {
        Setup.SetupHelper.RegisterAssembly(typeof(Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(BuildFileInfo).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vhd.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vhdx.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vmdk.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vdi.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(WimFile).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Xva.Disk).Assembly);
    }
}