//
// Copyright (c) 2008-2011, Kenneth Bell
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//

namespace DiscUtils.Archives
{
    using Internal;
    using Streams;
    using System;
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    /// Minimal tar file format implementation.
    /// </summary>
    public class TarFile : IDisposable
    {
        public static int FileDatabufferChunkSize { get; set; } = 32 * 1024 * 1024;

        private readonly Stream _fileStream;
        private readonly Dictionary<string, TarFileRecord> _files;
        private bool disposedValue;

        /// <summary>
        /// Initializes a new instance of the TarFile class.
        /// </summary>
        /// <param name="fileStream">The Tar file.</param>
        public TarFile(Stream fileStream)
        {
            _fileStream = fileStream;
            _files = new Dictionary<string, TarFileRecord>(StringComparer.Ordinal);

            var hdrBuf = new byte[512];

            string long_path = null;

            for (;;)
            {                
                if (StreamUtilities.ReadMaximum(_fileStream, hdrBuf, 0, 512) < 512)
                {
                    break;
                }

                var hdr = new TarHeader(hdrBuf, 0);

                if (hdr.FileLength == 0 && string.IsNullOrEmpty(hdr.FileName))
                {
                    break;
                }

                var record = new TarFileRecord(hdr, _fileStream.Position);
                if (hdr.FileType == UnixFileType.TarEntryLongLink &&
                    hdr.FileName.Equals("././@LongLink", StringComparison.Ordinal))
                {
                    var buffer = new byte[hdr.FileLength];
                    _fileStream.Read(buffer, 0, buffer.Length);
                    long_path = TarHeader.ReadNullTerminatedString(buffer, 0, buffer.Length).TrimEnd(' ');
                    _fileStream.Position += -(buffer.Length & 511) & 511;
                }
                else
                {
                    if (long_path is not null)
                    {
                        hdr.FileName = long_path;
                        long_path = null;
                    }
                    _files.Add(hdr.FileName, record);
                    _fileStream.Position += ((hdr.FileLength + 511) / 512) * 512;
                }
            }
        }

        /// <summary>
        /// Tries to open a file contained in the archive, if it exists.
        /// </summary>
        /// <param name="path">The path to the file within the archive.</param>
        /// <param name="stream">A stream containing the file contents, or null.</param>
        /// <returns><c>true</c> if the file could be opened, else <c>false</c>.</returns>
        public bool TryOpenFile(string path, out Stream stream)
        {
            if (_files.TryGetValue(path, out var file))
            {
                stream = new SubStream(_fileStream, file.Start, file.Length);
                return true;
            }

            stream = null;
            return false;
        }

        /// <summary>
        /// Open a file contained in the archive.
        /// </summary>
        /// <param name="path">The path to the file within the archive.</param>
        /// <returns>A stream containing the file contents.</returns>
        /// <exception cref="FileNotFoundException">Thrown if the file is not found.</exception>
        public Stream OpenFile(string path)
        {
            if (_files.TryGetValue(path, out var file))
            {
                return new SubStream(_fileStream, file.Start, file.Length);
            }

            throw new FileNotFoundException("File is not in archive", path);
        }

        public ICollection<string> GetFileNames()
        {
            return _files.Keys;
        }

        public ICollection<TarFileRecord> GetFiles()
        {
            return _files.Values;
        }

        /// <summary>
        /// Determines if a given file exists in the archive.
        /// </summary>
        /// <param name="path">The file path to test.</param>
        /// <returns><c>true</c> if the file is present, else <c>false</c>.</returns>
        public bool FileExists(string path)
        {
            return _files.ContainsKey(path);
        }

        /// <summary>
        /// Determines if a given directory exists in the archive.
        /// </summary>
        /// <param name="path">The file path to test.</param>
        /// <returns><c>true</c> if the directory is present, else <c>false</c>.</returns>
        public bool DirExists(string path)
        {
            string searchStr = path;
            searchStr = searchStr.Replace(@"\", "/");
            searchStr = searchStr.EndsWith(@"/", StringComparison.Ordinal) ? searchStr : searchStr + @"/";

            foreach (string filePath in _files.Keys)
            {
                if (filePath.StartsWith(searchStr, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        public IEnumerable<TarFileRecord> GetFiles(string dir)
        {
            string searchStr = dir;
            searchStr = searchStr.Replace(@"\", "/");
            searchStr = searchStr.EndsWith(@"/", StringComparison.Ordinal) ? searchStr : searchStr + @"/";

            foreach (var filePath in _files.Keys)
            {
                if (filePath.StartsWith(searchStr, StringComparison.Ordinal))
                {
                    yield return _files[filePath];
                }
            }
        }

        public static IEnumerable<TarFileData> EnumerateFiles(Stream archive)
        {
            var hdrBuf = new byte[512];

            string long_path = null;

            for (;;)
            {
                if (StreamUtilities.ReadMaximum(archive, hdrBuf, 0, 512) < 512)
                {
                    break;
                }

                var hdr = new TarHeader(hdrBuf, 0);

                if (long_path is not null)
                {
                    hdr.FileName = long_path;
                    long_path = null;
                }

                if (hdr.FileLength == 0 && string.IsNullOrEmpty(hdr.FileName))
                {
                    break;
                }

                if (hdr.FileLength == 0)
                {
                    yield return new TarFileData(hdr, source: null);
                }
                else if (hdr.FileType == UnixFileType.TarEntryLongLink &&
                    hdr.FileName.Equals("././@LongLink", StringComparison.Ordinal))
                {
                    var data = new byte[hdr.FileLength];

                    if (archive.Read(data, 0, data.Length) < hdr.FileLength)
                    {
                        throw new EndOfStreamException("Unexpected end of tar stream");
                    }

                    long_path = TarHeader.ReadNullTerminatedString(data, 0, data.Length).TrimEnd(' ');

                    var moveForward = (int)(-(hdr.FileLength & 511) & 511);

                    if (archive.Read(hdrBuf, 0, moveForward) < moveForward)
                    {
                        break;
                    }
                }
                else if (archive.CanSeek)
                {
                    var location = archive.Position;

                    var datastream = new SubStream(archive, location, hdr.FileLength);

                    yield return new TarFileData(hdr, datastream);

                    archive.Position = location + hdr.FileLength + (-(datastream.Length & 511) & 511);
                }
                else
                {
                    Stream datastream;

                    if (hdr.FileLength >= FileDatabufferChunkSize)
                    {
                        var data = new SparseMemoryBuffer(FileDatabufferChunkSize);

                        if (data.WriteFromStream(0, archive, hdr.FileLength) < hdr.FileLength)
                        {
                            throw new EndOfStreamException("Unexpected end of tar stream");
                        }

                        datastream = new SparseMemoryStream(data, FileAccess.Read);
                    }
                    else
                    {
                        var data = new byte[hdr.FileLength];

                        if (archive.Read(data, 0, data.Length) < hdr.FileLength)
                        {
                            throw new EndOfStreamException("Unexpected end of tar stream");
                        }

                        datastream = new MemoryStream(data, writable: false);
                    }

                    yield return new TarFileData(hdr, datastream);

                    var moveForward = (int)(-(datastream.Length & 511) & 511);

                    if (archive.Read(hdrBuf, 0, moveForward) < moveForward)
                    {
                        break;
                    }
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                    _fileStream.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer

                // TODO: set large fields to null
                _files.Clear();

                disposedValue = true;
            }
        }

        // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        ~TarFile()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
