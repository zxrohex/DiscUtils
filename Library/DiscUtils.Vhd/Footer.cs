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
using DiscUtils.Streams;

namespace DiscUtils.Vhd;

internal class Footer
{
    public const string FileCookie = "conectix";
    public const uint FeatureNone = 0x0;
    public const uint FeatureTemporary = 0x1;
    public const uint FeatureReservedMustBeSet = 0x2;
    public const uint Version1 = 0x00010000;
    public const uint Version6Point1 = 0x00060001;
    public const string VirtualPCSig = "vpc ";
    public const string VirtualServerSig = "vs  ";
    public const uint VirtualPC2004Version = 0x00050000;
    public const uint VirtualServer2004Version = 0x00010000;
    public const string WindowsHostOS = "Wi2k";
    public const string MacHostOS = "Mac ";
    public const uint CylindersMask = 0x0000FFFF;
    public const uint HeadsMask = 0x00FF0000;
    public const uint SectorsMask = 0xFF000000;

    public static readonly DateTime EpochUtc = new DateTime(2000, 1, 1, 0, 0, 0, 0);
    public uint Checksum;

    public string Cookie;
    public string CreatorApp;
    public string CreatorHostOS;
    public uint CreatorVersion;
    public long CurrentSize;
    public long DataOffset;
    public FileType DiskType;
    public uint Features;
    public uint FileFormatVersion;
    public Geometry Geometry;
    public long OriginalSize;
    public byte SavedState;
    public DateTime Timestamp;
    public Guid UniqueId;

    public Footer(Geometry geometry, long capacity, FileType type)
    {
        Cookie = FileCookie;
        Features = FeatureReservedMustBeSet;
        FileFormatVersion = Version1;
        DataOffset = -1;
        Timestamp = DateTime.UtcNow;
        CreatorApp = "dutl";
        CreatorVersion = Version6Point1;
        CreatorHostOS = WindowsHostOS;
        OriginalSize = capacity;
        CurrentSize = capacity;
        Geometry = geometry;
        DiskType = type;
        UniqueId = Guid.NewGuid();
        ////SavedState = 0;
    }

    public Footer(Footer toCopy)
    {
        Cookie = toCopy.Cookie;
        Features = toCopy.Features;
        FileFormatVersion = toCopy.FileFormatVersion;
        DataOffset = toCopy.DataOffset;
        Timestamp = toCopy.Timestamp;
        CreatorApp = toCopy.CreatorApp;
        CreatorVersion = toCopy.CreatorVersion;
        CreatorHostOS = toCopy.CreatorHostOS;
        OriginalSize = toCopy.OriginalSize;
        CurrentSize = toCopy.CurrentSize;
        Geometry = toCopy.Geometry;
        DiskType = toCopy.DiskType;
        Checksum = toCopy.Checksum;
        UniqueId = toCopy.UniqueId;
        SavedState = toCopy.SavedState;
    }

    private Footer() {}

    public bool IsValid()
    {
        return (Cookie == FileCookie)
               && IsChecksumValid()
               ////&& ((Features & FeatureReservedMustBeSet) != 0)
               && FileFormatVersion == Version1;
    }

    public bool IsChecksumValid()
    {
        return Checksum == CalculateChecksum();
    }

    public uint UpdateChecksum()
    {
        Checksum = CalculateChecksum();
        return Checksum;
    }

    private uint CalculateChecksum()
    {
        var copy = new Footer(this)
        {
            Checksum = 0
        };

        var asBytes = new byte[512];
        copy.ToBytes(asBytes, 0);
        uint checksum = 0;
        foreach (uint value in asBytes)
        {
            checksum += value;
        }

        checksum = ~checksum;

        return checksum;
    }

    #region Marshalling

    public static Footer FromBytes(byte[] buffer, int offset)
    {
        var result = new Footer
        {
            Cookie = EndianUtilities.BytesToString(buffer, offset + 0, 8),
            Features = EndianUtilities.ToUInt32BigEndian(buffer, offset + 8),
            FileFormatVersion = EndianUtilities.ToUInt32BigEndian(buffer, offset + 12),
            DataOffset = EndianUtilities.ToInt64BigEndian(buffer, offset + 16),
            Timestamp = EpochUtc.AddSeconds(EndianUtilities.ToUInt32BigEndian(buffer, offset + 24)),
            CreatorApp = EndianUtilities.BytesToString(buffer, offset + 28, 4),
            CreatorVersion = EndianUtilities.ToUInt32BigEndian(buffer, offset + 32),
            CreatorHostOS = EndianUtilities.BytesToString(buffer, offset + 36, 4),
            OriginalSize = EndianUtilities.ToInt64BigEndian(buffer, offset + 40),
            CurrentSize = EndianUtilities.ToInt64BigEndian(buffer, offset + 48),
            Geometry = new Geometry(EndianUtilities.ToUInt16BigEndian(buffer, offset + 56), buffer[58], buffer[59]),
            DiskType = (FileType)EndianUtilities.ToUInt32BigEndian(buffer, offset + 60),
            Checksum = EndianUtilities.ToUInt32BigEndian(buffer, offset + 64),
            UniqueId = EndianUtilities.ToGuidBigEndian(buffer, offset + 68),
            SavedState = buffer[84]
        };

        return result;
    }

    public void ToBytes(byte[] buffer, int offset)
    {
        EndianUtilities.StringToBytes(Cookie, buffer, offset + 0, 8);
        EndianUtilities.WriteBytesBigEndian(Features, buffer, offset + 8);
        EndianUtilities.WriteBytesBigEndian(FileFormatVersion, buffer, offset + 12);
        EndianUtilities.WriteBytesBigEndian(DataOffset, buffer, offset + 16);
        EndianUtilities.WriteBytesBigEndian((uint)(Timestamp - EpochUtc).TotalSeconds, buffer, offset + 24);
        EndianUtilities.StringToBytes(CreatorApp, buffer, offset + 28, 4);
        EndianUtilities.WriteBytesBigEndian(CreatorVersion, buffer, offset + 32);
        EndianUtilities.StringToBytes(CreatorHostOS, buffer, offset + 36, 4);
        EndianUtilities.WriteBytesBigEndian(OriginalSize, buffer, offset + 40);
        EndianUtilities.WriteBytesBigEndian(CurrentSize, buffer, offset + 48);
        EndianUtilities.WriteBytesBigEndian((ushort)Geometry.Cylinders, buffer, offset + 56);
        buffer[offset + 58] = (byte)Geometry.HeadsPerCylinder;
        buffer[offset + 59] = (byte)Geometry.SectorsPerTrack;
        EndianUtilities.WriteBytesBigEndian((uint)DiskType, buffer, offset + 60);
        EndianUtilities.WriteBytesBigEndian(Checksum, buffer, offset + 64);
        EndianUtilities.WriteBytesBigEndian(UniqueId, buffer, offset + 68);
        buffer[84] = SavedState;
        Array.Clear(buffer, 85, 427);
    }

    #endregion
}