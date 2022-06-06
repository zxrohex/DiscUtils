using System;
using System.IO;

namespace DiscUtils;

public class CachedDiscDirectoryInfo : DiscDirectoryInfo
{
    private FileAttributes attributes;
    private DateTime creationTimeUtc;
    private DateTime lastAccessTimeUtc;
    private DateTime lastWriteTimeUtc;
    private bool exists;

    public CachedDiscDirectoryInfo(DiscFileSystem fileSystem, string path, FileAttributes attributes,
                               DateTime creationTimeUtc, DateTime lastAccessTimeUtc, DateTime lastWriteTimeUtc)
        : base(fileSystem, path)
    {
        this.attributes = attributes;
        this.creationTimeUtc = creationTimeUtc;
        this.lastAccessTimeUtc = lastAccessTimeUtc;
        this.lastWriteTimeUtc = lastWriteTimeUtc;
        exists = true;
    }

    public CachedDiscDirectoryInfo(DiscFileSystem fileSystem, string path)
        : base(fileSystem, path)
    {
        exists = false;
    }

    public override FileAttributes Attributes
    {
        get => attributes; set
        {
            base.Attributes = value;
            attributes = value;
        }
    }

    public override DateTime CreationTimeUtc
    {
        get => creationTimeUtc; set
        {
            base.CreationTimeUtc = value;
            creationTimeUtc = value;
        }
    }

    public override DateTime LastAccessTimeUtc
    {
        get => lastAccessTimeUtc; set
        {
            base.LastAccessTimeUtc = value;
            lastAccessTimeUtc = value;
        }
    }

    public override DateTime LastWriteTimeUtc
    {
        get => lastWriteTimeUtc; set
        {
            base.LastWriteTimeUtc = value;
            lastWriteTimeUtc = value;
        }
    }

    public override void Delete()
    {
        base.Delete();
        exists = false;
    }

    public override void Create()
    {
        base.Create();
        exists = true;
    }

    public override bool Exists => exists;
}
