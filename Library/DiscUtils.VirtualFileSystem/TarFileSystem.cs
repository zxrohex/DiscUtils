using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DiscUtils.VirtualFileSystem
{
    using Archives;
    using Internal;

    public class TarFileSystem : VirtualFileSystem
    {
        private readonly WeakReference _tar;

        public TarFileSystem(FileStream tar_stream, bool ownsStream)
            : this(tar_stream, tar_stream.Name, ownsStream) { }

        public TarFileSystem(Stream tar_stream, string label, bool ownsStream)
            : base(new VirtualFileSystemOptions
            {
                VolumeLabel = label
            })
        {
            if (ownsStream)
            {
                _tar = new WeakReference(tar_stream);
            }

            foreach (var file in TarFile.EnumerateFiles(tar_stream))
            {
                var path = file.Name;

                if (path.StartsWith("./", StringComparison.Ordinal))
                {
                    path = path.Substring(2);
                }

                path = path.Replace('/', '\\');

                if (path.EndsWith(@"\", StringComparison.Ordinal))
                {
                    path = path.TrimEnd('\\');

                    AddDirectory(path,
                        file.Header.CreationTime.DateTime, file.Header.ModificationTime.DateTime, file.Header.LastAccessTime.DateTime,
                        Utilities.FileAttributesFromUnixFilePermissions(path, file.Header.FileMode, file.Header.FileType));
                }
                else
                {
                    AddFile(path, file.GetStream(),
                        file.Header.CreationTime.DateTime, file.Header.ModificationTime.DateTime, file.Header.LastAccessTime.DateTime,
                        Utilities.FileAttributesFromUnixFilePermissions(path, file.Header.FileMode, file.Header.FileType));
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && _tar?.Target is IDisposable archive)
            {
                archive.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
