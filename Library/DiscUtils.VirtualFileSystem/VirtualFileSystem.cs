using System;
using System.IO;
using System.Linq;

namespace DiscUtils.VirtualFileSystem
{
    using Internal;
    using Streams;

    public partial class VirtualFileSystem : DiscFileSystem, IFileSystemBuilder
    {
        public delegate Stream FileOpenDelegate(FileMode mode, FileAccess access);

        private readonly VirtualFileSystemDirectory _root;

        private long _used_space;

        public event EventHandler<CreateFileEventArgs> CreateFile;

        public new VirtualFileSystemOptions Options => base.Options as VirtualFileSystemOptions;

        public VirtualFileSystem(VirtualFileSystemOptions options)
            : base(options) =>
            _root = new VirtualFileSystemDirectory(this);

        public override bool CanWrite => Options.CanWrite;

        public override bool IsThreadSafe => Options.IsThreadSafe;

        public override string VolumeLabel => Options.VolumeLabel;

        public override string FriendlyName => "VirtualFileSystem";

        public override long Size => _used_space;

        public override long UsedSpace => _used_space;

        public override long AvailableSpace => 0;

        int IFileSystemBuilder.FileCount => throw new NotImplementedException();

        long IFileSystemBuilder.TotalSize => throw new NotImplementedException();

        string IFileSystemBuilder.VolumeIdentifier { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void SetUsedSpace(long size) => _used_space = size;

        public long UpdateUsedSpace() =>
            _used_space = _root.EnumerateTreeEntries()
            .OfType<VirtualFileSystemFile>()
            .Sum(file => file.AllocationLength);

        public override void CopyFile(string sourceFile, string destinationFile, bool overwrite)
        {
            throw new NotImplementedException();
        }

        public VirtualFileSystemDirectoryEntry AddLink(string existing, string new_path)
        {
            if (!CanWrite)
            {
                throw new IOException("Volume is not writable");
            }

            var entry = _root.ResolvePathToEntry(existing) ?? throw new FileNotFoundException("File not found", existing);

            var destination = _root.CreateSubDirectoryTree(GetPathDirectoryName(new_path));

            return entry.AddLink(destination, Path.GetFileName(new_path));
        }

        public VirtualFileSystemDirectoryEntry AddLink(VirtualFileSystemDirectoryEntry entry, string new_path)
        {
            if (!CanWrite)
            {
                throw new IOException("Volume is not writable");
            }

            var destination = _root.CreateSubDirectoryTree(GetPathDirectoryName(new_path));

            return entry.AddLink(destination, Path.GetFileName(new_path));
        }

        public override void CreateDirectory(string path) => AddDirectory(path);

        public VirtualFileSystemDirectory AddDirectory(string path)
        {
            if (!CanWrite)
            {
                throw new IOException("Volume is not writable");
            }

            return _root.CreateSubDirectoryTree(path);
        }

        public override void DeleteDirectory(string path, bool recursive)
        {
            if (!CanWrite)
            {
                throw new IOException("Volume is not writable");
            }

            if (!(_root.ResolvePathToEntry(path) is VirtualFileSystemDirectory directory))
            {
                throw new DirectoryNotFoundException();
            }

            directory.Delete(recursive);
        }

        public override void DeleteDirectory(string path) => DeleteDirectory(path, recursive: false);

        public override void DeleteFile(string path)
        {
            if (!CanWrite)
            {
                throw new IOException("Volume is not writable");
            }

            if (!(_root.ResolvePathToEntry(path) is VirtualFileSystemFile file))
            {
                throw new FileNotFoundException("File not found", path);
            }

            file.Delete();
        }

        public override bool DirectoryExists(string path) => _root.ResolvePathToEntry(path) is VirtualFileSystemDirectory;

        public override bool FileExists(string path) => _root.ResolvePathToEntry(path) is VirtualFileSystemFile;

        public override bool Exists(string path) => _root.ResolvePathToEntry(path) != null;

        public override string[] GetDirectories(string path, string searchPattern, SearchOption searchOption)
        {
            var directory = _root.ResolvePathToEntry(path) as VirtualFileSystemDirectory ??
                throw new DirectoryNotFoundException();

            var filter = GetFilter(searchPattern);

            if (searchOption == SearchOption.TopDirectoryOnly)
            {
                return directory.EnumerateDirectories()
                    .Select(entry => entry.Key)
                    .Where(filter)
                    .Select(name => Path.Combine(path, name))
                    .ToArray();
            }

            return directory.EnumerateTree()
                .Where(entry => entry.Value is VirtualFileSystemDirectory)
                .Select(entry => entry.Key)
                .Where(name => filter(Path.GetFileName(name)))
                .Select(name => Path.Combine(path, name))
                .ToArray();
        }

        public static Func<string, bool> GetFilter(string searchPattern)
        {
            if (string.IsNullOrEmpty(searchPattern) || searchPattern.Equals("*", StringComparison.Ordinal) || searchPattern.Equals("*.*", StringComparison.Ordinal))
            {
                return name => true;
            }
            else
            {
                return Utilities.ConvertWildcardsToRegEx(searchPattern).IsMatch;
            }
        }

        public override string[] GetFiles(string path, string searchPattern, SearchOption searchOption)
        {
            var directory = _root.ResolvePathToEntry(path) as VirtualFileSystemDirectory ??
                throw new DirectoryNotFoundException();

            var filter = GetFilter(searchPattern);

            if (searchOption == SearchOption.TopDirectoryOnly)
            {
                return directory.EnumerateFiles()
                    .Select(entry => entry.Key)
                    .Where(filter)
                    .Select(name => Path.Combine(path, name))
                    .ToArray();
            }

            return directory.EnumerateTree()
                .Where(entry => entry.Value is VirtualFileSystemFile)
                .Select(entry => entry.Key)
                .Where(name => filter(Path.GetFileName(name)))
                .Select(name => Path.Combine(path, name))
                .ToArray();
        }

        public override string[] GetFileSystemEntries(string path)
        {
            var directory = _root.ResolvePathToEntry(path) as VirtualFileSystemDirectory ??
                throw new DirectoryNotFoundException();

            return directory.GetNames()
                .Select(name => Path.Combine(path, name))
                .ToArray();
        }

        public override string[] GetFileSystemEntries(string path, string searchPattern)
        {
            var directory = _root.ResolvePathToEntry(path) as VirtualFileSystemDirectory ??
                throw new DirectoryNotFoundException();

            return directory.GetNames()
                .Where(GetFilter(searchPattern))
                .Select(name => Path.Combine(path, name))
                .ToArray();
        }

        public override void MoveDirectory(string sourceDirectoryName, string destinationDirectoryName)
        {
            var directory = _root.ResolvePathToEntry(sourceDirectoryName) as VirtualFileSystemDirectory ??
                throw new DirectoryNotFoundException();

            var destination = AddDirectory(GetPathDirectoryName(destinationDirectoryName));

            directory.Move(destination, Path.GetFileName(destinationDirectoryName), replace: false);
        }

        public override void MoveFile(string sourceName, string destinationName, bool overwrite)
        {
            var file = _root.ResolvePathToEntry(sourceName) as VirtualFileSystemFile ??
                throw new FileNotFoundException("File not found", sourceName);

            var destination = AddDirectory(GetPathDirectoryName(destinationName));

            file.Move(destination, Path.GetFileName(destinationName), overwrite);
        }

        public VirtualFileSystemFile AddFile(string path, byte[] data) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                (mode, access) => new MemoryStream(data, access.HasFlag(FileAccess.Write)))
            {
                Length = data.Length
            };

        public VirtualFileSystemFile AddFile(string path, byte[] data, int index, int count) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                (mode, access) => new MemoryStream(data, index, count, access.HasFlag(FileAccess.Write)))
            {
                Length = count
            };

        public VirtualFileSystemFile AddFile(string path, FileOpenDelegate open) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                open);

        public VirtualFileSystemFile AddFile(string path, DiscFileInfo existingFile) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                existingFile.Open)
            {
                Attributes = existingFile.Attributes,
                CreationTimeUtc = existingFile.CreationTimeUtc,
                LastAccessTimeUtc = existingFile.LastAccessTimeUtc,
                LastWriteTimeUtc = existingFile.LastWriteTimeUtc,
                Length = existingFile.Length
            };

        public VirtualFileSystemFile AddFile(string path, IFileSystem fileSystem, string existingPath) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                (mode, access) => fileSystem.OpenFile(existingPath, mode, access))
            {
                Attributes = fileSystem.GetAttributes(existingPath),
                CreationTimeUtc = fileSystem.GetCreationTimeUtc(existingPath),
                LastAccessTimeUtc = fileSystem.GetLastAccessTimeUtc(existingPath),
                LastWriteTimeUtc = fileSystem.GetLastWriteTimeUtc(existingPath),
                Length = fileSystem.GetFileLength(existingPath)
            };

        public VirtualFileSystemFile AddFile(string path, string existingPhysicalPath) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                (mode, access) => File.Open(existingPhysicalPath, mode, access))
            {
                Attributes = File.GetAttributes(existingPhysicalPath),
                CreationTimeUtc = File.GetCreationTimeUtc(existingPhysicalPath),
                LastAccessTimeUtc = File.GetLastAccessTimeUtc(existingPhysicalPath),
                LastWriteTimeUtc = File.GetLastWriteTimeUtc(existingPhysicalPath),
                Length = new FileInfo(existingPhysicalPath).Length
            };

        public void AddDirectory(string name, DateTime creationTime, DateTime writtenTime, DateTime accessedTime, FileAttributes attributes)
        {
            var member = AddDirectory(name);
            member.Attributes = attributes;
            member.CreationTimeUtc = creationTime.ToUniversalTime();
            member.LastWriteTimeUtc = writtenTime.ToUniversalTime();
            member.LastAccessTimeUtc = accessedTime.ToUniversalTime();
        }


        public void AddFile(string path, byte[] content, DateTime creationTime, DateTime writtenTime, DateTime accessedTime, FileAttributes attributes) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                (mode, access) => new MemoryStream(content, access.HasFlag(FileAccess.Write)))
            {
                Attributes = attributes,
                CreationTimeUtc = creationTime.ToUniversalTime(),
                LastAccessTimeUtc = creationTime.ToUniversalTime(),
                LastWriteTimeUtc = creationTime.ToUniversalTime(),
                Length = content.Length
            };

        public void AddFile(string path, Stream source, DateTime creationTime, DateTime writtenTime, DateTime accessedTime, FileAttributes attributes) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path), (mode, access) => SparseStream.FromStream(source, Ownership.None))
            {
                Attributes = attributes,
                CreationTimeUtc = creationTime.ToUniversalTime(),
                LastAccessTimeUtc = creationTime.ToUniversalTime(),
                LastWriteTimeUtc = creationTime.ToUniversalTime(),
                Length = source.Length
            };

        public void AddFile(string path, string existingPhysicalPath, DateTime creationTime, DateTime writtenTime, DateTime accessedTime, FileAttributes attributes) =>
            new VirtualFileSystemFile(AddDirectory(GetPathDirectoryName(path)),
                Path.GetFileName(path),
                (mode, access) => File.Open(existingPhysicalPath, mode, access))
            {
                Attributes = attributes,
                CreationTimeUtc = creationTime.ToUniversalTime(),
                LastAccessTimeUtc = creationTime.ToUniversalTime(),
                LastWriteTimeUtc = creationTime.ToUniversalTime(),
                Length = new FileInfo(existingPhysicalPath).Length
            };

        public override SparseStream OpenFile(string path, FileMode mode, FileAccess access)
        {
            if (!(_root.ResolvePathToEntry(path) is VirtualFileSystemFile file))
            {
                if (mode == FileMode.Open)
                {
                    throw new FileNotFoundException("File not found", path);
                }

                var e = new CreateFileEventArgs
                {
                    Path = path,
                    Mode = mode,
                    Access = access
                };

                OnCreateFile(e);

                file = e.Result;

                if (file == null)
                {
                    throw new NotImplementedException($"Could not create file '{path}'.");
                }
            }

            return file.Open(mode, access);
        }

        protected virtual void OnCreateFile(CreateFileEventArgs e) => CreateFile?.Invoke(this, e);

        public override FileAttributes GetAttributes(string path)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            return file.Attributes;
        }

        public override void SetAttributes(string path, FileAttributes newValue)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            file.Attributes = newValue;
        }

        public override DateTime GetCreationTimeUtc(string path)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            return file.CreationTimeUtc;
        }

        public override void SetCreationTimeUtc(string path, DateTime newTime)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            file.CreationTimeUtc = newTime;
        }

        public override DateTime GetLastAccessTimeUtc(string path)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            return file.LastAccessTimeUtc;
        }

        public override void SetLastAccessTimeUtc(string path, DateTime newTime)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            file.LastAccessTimeUtc = newTime;
        }

        public override DateTime GetLastWriteTimeUtc(string path)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            return file.LastWriteTimeUtc;
        }

        public override void SetLastWriteTimeUtc(string path, DateTime newTime)
        {
            var file = _root.ResolvePathToEntry(path) ??
                throw new FileNotFoundException("File not found", path);

            file.LastWriteTimeUtc = newTime;
        }

        public override long GetFileLength(string path)
        {
            var file = _root.ResolvePathToEntry(path) as VirtualFileSystemFile ??
                throw new FileNotFoundException("File not found", path);

            return file.Length;
        }

        public void Freeze()
        {
            Options.CanWrite = false;
            UpdateUsedSpace();
        }

        public override string ToString() => VolumeLabel ?? FriendlyName ?? base.ToString();

        void IFileSystemBuilder.AddDirectory(string name) =>
            AddDirectory(name);

        void IFileSystemBuilder.AddDirectory(string name, int ownerId, int groupId, UnixFilePermissions fileMode, DateTime modificationTime)
        {
            var member = AddDirectory(name);
            
            member.LastWriteTimeUtc = modificationTime.ToUniversalTime();
            
            if (!fileMode.HasFlag(UnixFilePermissions.OwnerWrite))
            {
                member.Attributes |= FileAttributes.ReadOnly;
            }
        }

        void IFileSystemBuilder.AddFile(string name, byte[] content) =>
            AddFile(name, content);

        void IFileSystemBuilder.AddFile(string name, byte[] content, int ownerId, int groupId, UnixFilePermissions fileMode, DateTime modificationTime)
        {
            var member = AddFile(name, content);
            
            member.LastWriteTimeUtc = modificationTime.ToUniversalTime();
            member.Attributes = Utilities.FileAttributesFromUnixFilePermissions(name, fileMode, UnixFileType.Regular);
        }

        void IFileSystemBuilder.AddFile(string name, Stream source)
        {
            var member = AddFile(name, (mode, access) => SparseStream.FromStream(source, Ownership.None));

            member.Length = source.Length;

            if (!source.CanWrite)
            {
                member.Attributes |= FileAttributes.ReadOnly;
            }
        }

        void IFileSystemBuilder.AddFile(string name, Stream source, int ownerId, int groupId, UnixFilePermissions fileMode, DateTime modificationTime)
        {
            var member = AddFile(name, (mode, access) => SparseStream.FromStream(source, Ownership.None));

            member.Length = source.Length;
            member.LastWriteTimeUtc = modificationTime;
            
            if (!fileMode.HasFlag(UnixFilePermissions.OwnerRead) || !source.CanWrite)
            {
                member.Attributes |= FileAttributes.ReadOnly;
            }
        }

        void IFileSystemBuilder.AddFile(string name, string sourcePath) =>
            AddFile(name, sourcePath);

        void IFileSystemBuilder.AddFile(string name, string sourcePath, int ownerId, int groupId, UnixFilePermissions fileMode, DateTime modificationTime)
        {
            var member = AddFile(name, sourcePath);
            
            member.LastWriteTimeUtc = modificationTime.ToUniversalTime();

            if (!fileMode.HasFlag(UnixFilePermissions.OwnerRead))
            {
                member.Attributes |= FileAttributes.ReadOnly;
            }
        }

        IFileSystem IFileSystemBuilder.GenerateFileSystem() => this;

        public static string GetPathDirectoryName(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return string.Empty;
            }

            var index = path.LastIndexOfAny(new[] { '\\', '/' });

            if (index >= 0)
            {
                return path.Remove(index);
            }

            return string.Empty;
        }
    }
}
