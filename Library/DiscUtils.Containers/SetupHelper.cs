namespace DiscUtils.Containers;

public static class SetupHelper
{
    public static void SetupContainers()
    {
        Setup.SetupHelper.RegisterAssembly(typeof(Dmg.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Iso9660.BuildFileInfo).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Lvm.LogicalVolumeManager).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vhd.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vhdx.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vmdk.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Vdi.Disk).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Wim.WimFile).Assembly);
        Setup.SetupHelper.RegisterAssembly(typeof(Xva.Disk).Assembly);
    }
}