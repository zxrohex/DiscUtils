using System;
using System.IO;

namespace DiscUtils.VirtualFileSystem;

public class CreateFileEventArgs : EventArgs
{
    public string Path { get; set; }
    public FileMode Mode { get; set; }
    public FileAccess Access { get; set; }
    public VirtualFileSystemFile Result { get; set; }
}