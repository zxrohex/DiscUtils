using System;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace DiscUtils.VirtualFileSystem
{
    using Archives;
    using Streams;
    using Internal;

#if NET45_OR_GREATER || NETSTANDARD2_0
    public class ZipFileSystem : VirtualFileSystem
    {
        private readonly WeakReference _zip;

        public int FileDatabufferChunkSize { get; set; } = 32 * 1024 * 1024;

        public static bool Detect(Stream archive)
        {
            archive.Position = 0;

            try
            {
                var buffer = new byte[512];

                if (StreamUtilities.ReadMaximum(archive, buffer, 0, 512) < 512)
                {
                    return false;
                }

                return BitConverter.ToUInt32(buffer, 0) == 0x04034B50U;
            }
            catch
            {
                return false;
            }
        }

        public ZipFileSystem(FileStream zip_stream, bool ownsStream)
            : this(zip_stream, zip_stream.Name, ownsStream) { }

        public ZipFileSystem(Stream zip_stream, string label, bool ownsStream)
            : base(new VirtualFileSystemOptions
            {
                VolumeLabel = label
            })
        {
            if (zip_stream.CanSeek)
            {
                zip_stream.Position = 0;
            }

            var zip = new ZipArchive(zip_stream, ZipArchiveMode.Read, leaveOpen: false);

            if (ownsStream)
            {
                _zip = new WeakReference(zip);
            }

            foreach (var file in zip.Entries)
            {
                var path = file.FullName;

                if (path.StartsWith(@"./", StringComparison.Ordinal) ||
                    path.StartsWith(@".\", StringComparison.Ordinal))
                {
                    path = path.Substring(2);
                }

                path = path.Replace('/', '\\');

                if (path.EndsWith(@"\", StringComparison.Ordinal))
                {
                    path = path.TrimEnd('\\');

                    AddDirectory(path,
                        file.LastWriteTime.DateTime, file.LastWriteTime.DateTime, file.LastWriteTime.DateTime, FileAttributes.Directory);
                }
                else
                {
                    var entry = AddFile(path, (mode, access) =>
                    {
                        Stream datastream;

                        if (file.Length >= FileDatabufferChunkSize)
                        {
                            var data = new SparseMemoryBuffer(FileDatabufferChunkSize);

                            if (data.WriteFromStream(0, file.Open(), file.Length) < file.Length)
                            {
                                throw new EndOfStreamException("Unexpected end of tar stream");
                            }

                            datastream = new SparseMemoryStream(data, FileAccess.Read);
                        }
                        else
                        {
                            var data = new byte[file.Length];

                            if (file.Open().Read(data, 0, data.Length) < file.Length)
                            {
                                throw new EndOfStreamException("Unexpected end of tar stream");
                            }

                            datastream = new MemoryStream(data, writable: false);
                        }

                        return datastream;
                    });

                    entry.Length = file.Length;
                    
                    entry.LastAccessTimeUtc =
                        entry.CreationTimeUtc =
                        entry.LastWriteTimeUtc =
                        file.LastWriteTime.UtcDateTime;
                }
            }

            Freeze();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && _zip?.Target is IDisposable archive)
            {
                archive.Dispose();
            }

            base.Dispose(disposing);
        }
    }
#endif
}
