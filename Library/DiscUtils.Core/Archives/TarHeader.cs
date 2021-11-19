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
    using System.Linq;

    public sealed class TarHeader
    {
        public const int Length = 512;

        public string FileName { get; internal set; }
        public UnixFilePermissions FileMode { get; }
        public int OwnerId { get; }
        public int GroupId { get; }
        public long FileLength { get; }
        public DateTimeOffset ModificationTime { get; }
        public int CheckSum { get; }
        public UnixFileType FileType { get; }
        public string LinkName { get; }
        public string Magic { get; }
        public int Version { get; }
        public string OwnerName { get; }
        public string GroupName { get; }
        public int DevMajor { get; }
        public int DevMinor { get; }
        public DateTimeOffset LastAccessTime { get; }
        public DateTimeOffset CreationTime { get; }

        public TarHeader()
        {
        }

        public TarHeader(string fileName, long fileLength, UnixFilePermissions fileMode, int ownerId, int groupId, DateTime modificationTime)
        {
            FileName = fileName;
            FileLength = fileLength;
            FileMode = fileMode;
            OwnerId = ownerId;
            GroupId = groupId;
            ModificationTime = modificationTime;
        }

        public TarHeader(byte[] buffer, int offset)
        {
            FileName = ReadNullTerminatedString(buffer, offset + 0, 100);
            FileMode = (UnixFilePermissions)OctalToLong(ReadNullTerminatedString(buffer, offset + 100, 8));
            OwnerId = (int)OctalToLong(ReadNullTerminatedString(buffer, offset + 108, 8));
            GroupId = (int)OctalToLong(ReadNullTerminatedString(buffer, offset + 116, 8));
            FileLength = ParseFileLength(buffer, offset + 124, 12);
            ModificationTime = DateTimeOffsetExtensions.FromUnixTimeSeconds((uint)OctalToLong(ReadNullTerminatedString(buffer, offset + 136, 12)));
            CheckSum = (int)OctalToLong(ReadNullTerminatedString(buffer, offset + 148, 8));
            FileType = (UnixFileType)buffer[offset + 156];
            LinkName = ReadNullTerminatedString(buffer, offset + 157, 100);
            Magic = ReadNullTerminatedString(buffer, offset + 257, 6);
            Version = (int)OctalToLong(ReadNullTerminatedString(buffer, offset + 263, 2));
            OwnerName = ReadNullTerminatedString(buffer, offset + 265, 32);
            GroupName = ReadNullTerminatedString(buffer, offset + 297, 32);
            DevMajor = (int)OctalToLong(ReadNullTerminatedString(buffer, offset + 329, 8));
            DevMinor = (int)OctalToLong(ReadNullTerminatedString(buffer, offset + 337, 8));

            var prefix = ReadNullTerminatedString(buffer, offset + 345, 131);

            if (!string.IsNullOrEmpty(prefix))
            {
                FileName = $"{prefix}/{FileName}";
            }

            LastAccessTime = DateTimeOffsetExtensions.FromUnixTimeSeconds((uint)OctalToLong(ReadNullTerminatedString(buffer, offset + 476, 12)));
            CreationTime = DateTimeOffsetExtensions.FromUnixTimeSeconds((uint)OctalToLong(ReadNullTerminatedString(buffer, offset + 488, 12)));
        }

        public static bool IsValid(byte[] buffer, int offset)
        {
            if (ReadNullTerminatedString(buffer, offset + 0, 100).Length > 0 &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 100, 8)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 108, 8)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 116, 8)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 136, 12)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 148, 8)) &&
                ReadNullTerminatedString(buffer, offset + 257, 6).Length > 0 &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 263, 2)) &&
                ReadNullTerminatedString(buffer, offset + 265, 32).Length > 0 &&
                ReadNullTerminatedString(buffer, offset + 297, 32).Length > 0 &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 329, 8)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 337, 8)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 476, 12)) &&
                IsValidOctalString(ReadNullTerminatedString(buffer, offset + 488, 12)))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private static bool IsValidOctalString(string v) =>
            string.IsNullOrEmpty(v) || v.All(c => c >= '0' && c <= '7');

        private static long ParseFileLength(byte[] buffer, int offset, int count)
        {
            if (BitConverter.ToInt32(buffer, offset) == 128)
            {
                return EndianUtilities.ToInt64BigEndian(buffer, offset + 4);
            }
            else
            {
                return OctalToLong(ReadNullTerminatedString(buffer, offset, count));
            }
        }

        public void WriteTo(byte[] buffer, int offset)
        {
            Array.Clear(buffer, offset, Length);

            if (FileName.Length < 100)
            {
                EndianUtilities.StringToBytes(FileName, buffer, offset, 99);
            }
            else
            {
                var split = FileName.LastIndexOf('/', Math.Min(130, FileName.Length - 2));

                if (split < 0 || FileName.Length - split > 99)
                {
                    throw new InvalidOperationException($"File name '{FileName}' too long for tar header");
                }

                var prefix = FileName.Remove(split);
                EndianUtilities.StringToBytes(prefix, buffer, offset + 345, 130);

                var filename = FileName.Substring(split + 1);
                EndianUtilities.StringToBytes(filename, buffer, offset, 99);
            }

            EndianUtilities.StringToBytes(LongToOctal((long)FileMode, 7), buffer, offset + 100, 7);
            EndianUtilities.StringToBytes(LongToOctal(OwnerId, 7), buffer, offset + 108, 7);
            EndianUtilities.StringToBytes(LongToOctal(GroupId, 7), buffer, offset + 116, 7);
            EndianUtilities.StringToBytes(LongToOctal(FileLength, 11), buffer, offset + 124, 11);
            EndianUtilities.StringToBytes(LongToOctal(DateTimeOffsetExtensions.ToUnixTimeSeconds(ModificationTime), 11), buffer, offset + 136, 11);

            // Checksum
            EndianUtilities.StringToBytes(new string(' ', 8), buffer, offset + 148, 8);
            long checkSum = 0;
            for (int i = 0; i < 512; ++i)
            {
                checkSum += buffer[offset + i];
            }

            EndianUtilities.StringToBytes(LongToOctal(checkSum, 7), buffer, offset + 148, 7);
            buffer[155] = 0;
        }

        private static string ReadNullTerminatedString(byte[] buffer, int offset, int length)
        {
            var z = Array.IndexOf(buffer, default, offset, length);

            if (z == offset)
            {
                return string.Empty;
            }
            else if (z > offset)
            {
                length = z - offset;
            }

            return EndianUtilities.BytesToString(buffer, offset, length).TrimEnd(' ');
        }

        private static long OctalToLong(string value) =>
            string.IsNullOrEmpty(value) ? 0 : Convert.ToInt64(value, 8);
        //{
        //    long result = 0;

        //    for (int i = 0; i < value.Length; ++i)
        //    {
        //        result = (result * 8) + (value[i] - '0');
        //    }

        //    return result;
        //}

        private static string LongToOctal(long value, int length) =>
            Convert.ToString(value, 8).PadLeft(length, '0');
        //{
        //    string result = string.Empty;

        //    while (value > 0)
        //    {
        //        result = ((char)('0' + (value % 8))) + result;
        //        value = value / 8;
        //    }

        //    return new string('0', length - result.Length) + result;
        //}

        public override string ToString() => FileName;
    }
}
