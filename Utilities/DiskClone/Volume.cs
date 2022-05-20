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

using System;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DiskClone;

internal sealed class Volume : IDisposable
{
    private SafeFileHandle _handle;
    private Stream _stream;

    public string Path { get; }

    public long Length { get; }

    public Volume(string path, long length)
    {
        Path = path.TrimEnd('\\');
        Length = length;

        if (!Path.StartsWith(@"\\"))
        {
            Path = @"\\.\" + Path;
        }

        _handle = NativeMethods.CreateFileW(Path, FileAccess.Read, FileShare.ReadWrite, IntPtr.Zero, FileMode.Open, 0, IntPtr.Zero);
        if (_handle.IsInvalid)
        {
            throw Win32Wrapper.GetIOExceptionForLastError();
        }

        // Enable reading the full contents of the volume (not just the region bounded by the file system)
        var bytesRet = 0;
        if (!NativeMethods.DeviceIoControl(_handle, NativeMethods.EIOControlCode.FsctlAllowExtendedDasdIo, IntPtr.Zero, 0, IntPtr.Zero, 0, ref bytesRet, IntPtr.Zero))
        {
            throw Win32Wrapper.GetIOExceptionForLastError();
        }
    }

    public void Dispose()
    {
        if (_stream != null)
        {
            _stream.Dispose();
            _stream = null;
        }

        if (!_handle.IsClosed)
        {
            _handle.Dispose();
        }
    }

    public Stream Content
    {
        get
        {
            if (_stream == null)
            {
                _stream = new VolumeStream(_handle);
            }

            return _stream;
        }
    }

    public NativeMethods.DiskExtent[] GetDiskExtents()
    {
        var numExtents = 1;
        var bufferSize = 8 + Marshal.SizeOf(typeof(NativeMethods.DiskExtent)) * numExtents;
        var buffer = new byte[bufferSize];

        var bytesRet = 0;
        if (!NativeMethods.DeviceIoControl(_handle, NativeMethods.EIOControlCode.VolumeGetDiskExtents, null, 0, buffer, bufferSize, ref bytesRet, IntPtr.Zero))
        {
            if (Marshal.GetLastWin32Error() != NativeMethods.ERROR_MORE_DATA)
            {
                throw Win32Wrapper.GetIOExceptionForLastError();
            }

            numExtents = BitConverter.ToInt32(buffer, 0);
            bufferSize = 8 + Marshal.SizeOf(typeof(NativeMethods.DiskExtent)) * numExtents;
            buffer = new byte[bufferSize];

            if (!NativeMethods.DeviceIoControl(_handle, NativeMethods.EIOControlCode.VolumeGetDiskExtents, null, 0, buffer, bufferSize, ref bytesRet, IntPtr.Zero))
            {
                throw Win32Wrapper.GetIOExceptionForLastError();
            }
        }

        return Win32Wrapper.ByteArrayToStructureArray<NativeMethods.DiskExtent>(buffer, 8, 1);
    }
}
