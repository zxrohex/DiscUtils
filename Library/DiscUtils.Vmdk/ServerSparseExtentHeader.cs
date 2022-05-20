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

using DiscUtils.Streams;

namespace DiscUtils.Vmdk;

internal class ServerSparseExtentHeader : CommonSparseExtentHeader
{
    public const uint CowdMagicNumber = 0x44574f43;

    public uint Flags;
    public uint FreeSector;
    public uint NumGdEntries;
    public uint SavedGeneration;
    public uint UncleanShutdown;

    public ServerSparseExtentHeader()
    {
        MagicNumber = CowdMagicNumber;
        Version = 1;
        GrainSize = 512;
        NumGTEsPerGT = 4096;
        Flags = 3;
    }

    public static ServerSparseExtentHeader Read(byte[] buffer, int offset)
    {
        var hdr = new ServerSparseExtentHeader
        {
            MagicNumber = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x00),
            Version = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x04),
            Flags = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x08),
            Capacity = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x0C),
            GrainSize = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x10),
            GdOffset = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x14),
            NumGdEntries = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x18),
            FreeSector = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x1C),

            SavedGeneration = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x660),
            UncleanShutdown = EndianUtilities.ToUInt32LittleEndian(buffer, offset + 0x66C),

            NumGTEsPerGT = 4096
        };

        return hdr;
    }

    public byte[] GetBytes()
    {
        var buffer = new byte[Sizes.Sector * 4];
        EndianUtilities.WriteBytesLittleEndian(MagicNumber, buffer, 0x00);
        EndianUtilities.WriteBytesLittleEndian(Version, buffer, 0x04);
        EndianUtilities.WriteBytesLittleEndian(Flags, buffer, 0x08);
        EndianUtilities.WriteBytesLittleEndian((uint)Capacity, buffer, 0x0C);
        EndianUtilities.WriteBytesLittleEndian((uint)GrainSize, buffer, 0x10);
        EndianUtilities.WriteBytesLittleEndian((uint)GdOffset, buffer, 0x14);
        EndianUtilities.WriteBytesLittleEndian(NumGdEntries, buffer, 0x18);
        EndianUtilities.WriteBytesLittleEndian(FreeSector, buffer, 0x1C);

        EndianUtilities.WriteBytesLittleEndian(SavedGeneration, buffer, 0x660);
        EndianUtilities.WriteBytesLittleEndian(UncleanShutdown, buffer, 0x66C);

        return buffer;
    }
}