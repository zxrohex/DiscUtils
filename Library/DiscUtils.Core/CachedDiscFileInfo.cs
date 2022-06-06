using System;
using System.IO;

namespace DiscUtils;

public class CachedDiscFileInfo : DiscFileInfo
{
    private FileAttributes attributes;
    private DateTime creationTimeUtc;
    private DateTime lastAccessTimeUtc;
    private DateTime lastWriteTimeUtc;
    private bool exists;

    public CachedDiscFileInfo(DiscFileSystem fileSystem, string path, FileAttributes attributes,
                              DateTime creationTimeUtc, DateTime lastAccessTimeUtc, DateTime lastWriteTimeUtc,
                              long length)
        : base(fileSystem, path)
    {
        this.attributes = attributes;
        this.creationTimeUtc = creationTimeUtc;
        this.lastAccessTimeUtc = lastAccessTimeUtc;
        this.lastWriteTimeUtc = lastWriteTimeUtc;
        exists = true;
        Length = length;
    }

    public CachedDiscFileInfo(DiscFileSystem fileSystem, string path)
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

    public override Stream Create()
    {
        var stream = base.Create();
        exists = true;
        return stream;
    }

    public override StreamWriter AppendText()
    {
        var wr = base.AppendText();
        exists = true;
        return wr;
    }

    public override Stream Open(FileMode mode)
    {
        var stream = base.Open(mode);
        exists = true;
        return stream;
    }

    public override Stream Open(FileMode mode, FileAccess access)
    {
        var stream = base.Open(mode, access);
        exists = true;
        return stream;
    }

    public override Stream OpenWrite()
    {
        var stream = base.OpenWrite();
        exists = true;
        return stream;
    }

    public override StreamReader OpenText()
    {
        var rd = base.OpenText();
        exists = true;
        return rd;
    }

    public override StreamWriter CreateText()
    {
        var wr = base.CreateText();
        exists = true;
        return wr;
    }

    public override void MoveTo(string destinationFileName)
    {
        base.MoveTo(destinationFileName);
        exists = false;
    }

    public override bool Exists => exists;

    public override long Length { get; }
}
