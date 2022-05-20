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
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace DiscUtils.Streams;

public static class EndianUtilities
{
    #region Bit Twiddling

    public static void WriteBytesLittleEndian(ushort val, byte[] buffer, int offset)
        => WriteBytesLittleEndian(val, buffer.AsSpan(offset, sizeof(ushort)));

    public static void WriteBytesLittleEndian(ushort val, Span<byte> buffer)
    {
        buffer[0] = (byte)(val & 0xFF);
        buffer[1] = (byte)((val >> 8) & 0xFF);
    }

    public static void WriteBytesLittleEndian(uint val, byte[] buffer, int offset)
        => WriteBytesLittleEndian(val, buffer.AsSpan(offset, sizeof(uint)));

    public static void WriteBytesLittleEndian(uint val, Span<byte> buffer)
    {
        buffer[0] = (byte)(val & 0xFF);
        buffer[1] = (byte)((val >> 8) & 0xFF);
        buffer[2] = (byte)((val >> 16) & 0xFF);
        buffer[3] = (byte)((val >> 24) & 0xFF);
    }

    public static void WriteBytesLittleEndian(ulong val, byte[] buffer, int offset)
        => WriteBytesLittleEndian(val, buffer.AsSpan(offset, sizeof(ulong)));
    
    public static void WriteBytesLittleEndian(ulong val, Span<byte> buffer)
    {
        buffer[0] = (byte)(val & 0xFF);
        buffer[1] = (byte)((val >> 8) & 0xFF);
        buffer[2] = (byte)((val >> 16) & 0xFF);
        buffer[3] = (byte)((val >> 24) & 0xFF);
        buffer[4] = (byte)((val >> 32) & 0xFF);
        buffer[5] = (byte)((val >> 40) & 0xFF);
        buffer[6] = (byte)((val >> 48) & 0xFF);
        buffer[7] = (byte)((val >> 56) & 0xFF);
    }

    public static void WriteBytesLittleEndian(short val, Span<byte> buffer)
    {
        WriteBytesLittleEndian((ushort)val, buffer);
    }

    public static void WriteBytesLittleEndian(short val, byte[] buffer, int offset)
    {
        WriteBytesLittleEndian((ushort)val, buffer, offset);
    }

    public static void WriteBytesLittleEndian(int val, Span<byte> buffer)
    {
        WriteBytesLittleEndian((uint)val, buffer);
    }

    public static void WriteBytesLittleEndian(int val, byte[] buffer, int offset)
    {
        WriteBytesLittleEndian((uint)val, buffer, offset);
    }

    public static void WriteBytesLittleEndian(long val, Span<byte> buffer)
    {
        WriteBytesLittleEndian((ulong)val, buffer);
    }

    public static void WriteBytesLittleEndian(long val, byte[] buffer, int offset)
    {
        WriteBytesLittleEndian((ulong)val, buffer, offset);
    }

    public static void WriteBytesLittleEndian(Guid val, Span<byte> buffer)
    {
        MemoryMarshal.Write(buffer, ref val);
    }

    public static void WriteBytesLittleEndian(Guid val, byte[] buffer, int offset)
    {
        MemoryMarshal.Write(buffer.AsSpan(offset), ref val);
    }

    public static void WriteBytesBigEndian(ushort val, byte[] buffer, int offset)
        => WriteBytesBigEndian(val, buffer.AsSpan(offset, sizeof(ushort)));

    public static void WriteBytesBigEndian(ushort val, Span<byte> buffer)
    {
        buffer[0] = (byte)(val >> 8);
        buffer[1] = (byte)(val & 0xFF);
    }

    public static void WriteBytesBigEndian(uint val, byte[] buffer, int offset)
        => WriteBytesBigEndian(val, buffer.AsSpan(offset, sizeof(uint)));

    public static void WriteBytesBigEndian(uint val, Span<byte> buffer)
    {
        buffer[0] = (byte)((val >> 24) & 0xFF);
        buffer[1] = (byte)((val >> 16) & 0xFF);
        buffer[2] = (byte)((val >> 8) & 0xFF);
        buffer[3] = (byte)(val & 0xFF);
    }

    public static void WriteBytesBigEndian(ulong val, byte[] buffer, int offset)
        => WriteBytesBigEndian(val, buffer.AsSpan(offset, sizeof(ulong)));

    public static void WriteBytesBigEndian(ulong val, Span<byte> buffer)
    {
        buffer[0] = (byte)((val >> 56) & 0xFF);
        buffer[1] = (byte)((val >> 48) & 0xFF);
        buffer[2] = (byte)((val >> 40) & 0xFF);
        buffer[3] = (byte)((val >> 32) & 0xFF);
        buffer[4] = (byte)((val >> 24) & 0xFF);
        buffer[5] = (byte)((val >> 16) & 0xFF);
        buffer[6] = (byte)((val >> 8) & 0xFF);
        buffer[7] = (byte)(val & 0xFF);
    }

    public static void WriteBytesBigEndian(short val, byte[] buffer, int offset)
    {
        WriteBytesBigEndian((ushort)val, buffer, offset);
    }

    public static void WriteBytesBigEndian(short val, Span<byte> buffer)
    {
        WriteBytesBigEndian((ushort)val, buffer);
    }

    public static void WriteBytesBigEndian(int val, Span<byte> buffer)
    {
        WriteBytesBigEndian((uint)val, buffer);
    }

    public static void WriteBytesBigEndian(int val, byte[] buffer, int offset)
    {
        WriteBytesBigEndian((uint)val, buffer, offset);
    }

    public static void WriteBytesBigEndian(long val, Span<byte> buffer)
    {
        WriteBytesBigEndian((ulong)val, buffer);
    }

    public static void WriteBytesBigEndian(long val, byte[] buffer, int offset)
    {
        WriteBytesBigEndian((ulong)val, buffer, offset);
    }

    public static void WriteBytesBigEndian(Guid val, byte[] buffer, int offset)
        => WriteBytesBigEndian(val, buffer.AsSpan(offset, 16));

    public static void WriteBytesBigEndian(Guid val, Span<byte> buffer)
    {
        MemoryMarshal.Write(buffer, ref val);
        WriteBytesBigEndian(ToUInt32LittleEndian(buffer.Slice(0, 4)), buffer.Slice(0, 4));
        WriteBytesBigEndian(ToUInt16LittleEndian(buffer.Slice(4, 2)), buffer.Slice(4, 2));
        WriteBytesBigEndian(ToUInt16LittleEndian(buffer.Slice(6, 2)), buffer.Slice(6, 2));
    }

    public static ushort ToUInt16LittleEndian(byte[] buffer, int offset)
    {
        return (ushort)(((buffer[offset + 1] << 8) & 0xFF00) | ((buffer[offset + 0] << 0) & 0x00FF));
    }

    public static ushort ToUInt16LittleEndian(ReadOnlySpan<byte> buffer)
    {
        return (ushort)(((buffer[1] << 8) & 0xFF00) | ((buffer[0] << 0) & 0x00FF));
    }

    public static uint ToUInt32LittleEndian(byte[] buffer, int offset)
    {
        return (uint)(((buffer[offset + 3] << 24) & 0xFF000000U) | ((buffer[offset + 2] << 16) & 0x00FF0000U)
                      | ((buffer[offset + 1] << 8) & 0x0000FF00U) | ((buffer[offset + 0] << 0) & 0x000000FFU));
    }

    public static uint ToUInt32LittleEndian(ReadOnlySpan<byte> buffer)
    {
        return (uint)(((buffer[3] << 24) & 0xFF000000U) | ((buffer[2] << 16) & 0x00FF0000U)
                      | ((buffer[1] << 8) & 0x0000FF00U) | ((buffer[0] << 0) & 0x000000FFU));
    }

    public static ulong ToUInt64LittleEndian(byte[] buffer, int offset)
    {
        return ((ulong)ToUInt32LittleEndian(buffer, offset + 4) << 32) | ToUInt32LittleEndian(buffer, offset + 0);
    }

    public static ulong ToUInt64LittleEndian(ReadOnlySpan<byte> buffer)
    {
        return ((ulong)ToUInt32LittleEndian(buffer.Slice(4, 4)) << 32) | ToUInt32LittleEndian(buffer.Slice(0, 4));
    }

    public static short ToInt16LittleEndian(byte[] buffer, int offset)
    {
        return (short)ToUInt16LittleEndian(buffer, offset);
    }

    public static short ToInt16LittleEndian(ReadOnlySpan<byte> buffer)
    {
        return (short)ToUInt16LittleEndian(buffer);
    }

    public static int ToInt32LittleEndian(byte[] buffer, int offset)
    {
        return (int)ToUInt32LittleEndian(buffer, offset);
    }

    public static int ToInt32LittleEndian(ReadOnlySpan<byte> buffer)
    {
        return (int)ToUInt32LittleEndian(buffer);
    }

    public static long ToInt64LittleEndian(byte[] buffer, int offset)
    {
        return (long)ToUInt64LittleEndian(buffer, offset);
    }

    public static long ToInt64LittleEndian(ReadOnlySpan<byte>  buffer)
    {
        return (long)ToUInt64LittleEndian(buffer);
    }

    public static ushort ToUInt16BigEndian(byte[] buffer, int offset)
    {
        return (ushort)(((buffer[offset] << 8) & 0xFF00) | ((buffer[offset + 1] << 0) & 0x00FF));
    }

    public static ushort ToUInt16BigEndian(ReadOnlySpan<byte> buffer)
    {
        return (ushort)(((buffer[0] << 8) & 0xFF00) | ((buffer[1] << 0) & 0x00FF));
    }

    public static uint ToUInt32BigEndian(byte[] buffer, int offset)
    {
        var val = (uint)(((buffer[offset + 0] << 24) & 0xFF000000U) | ((buffer[offset + 1] << 16) & 0x00FF0000U)
                          | ((buffer[offset + 2] << 8) & 0x0000FF00U) | ((buffer[offset + 3] << 0) & 0x000000FFU));
        return val;
    }

    public static uint ToUInt32BigEndian(ReadOnlySpan<byte> buffer)
    {
        var val = (uint)(((buffer[0] << 24) & 0xFF000000U) | ((buffer[1] << 16) & 0x00FF0000U)
                          | ((buffer[2] << 8) & 0x0000FF00U) | ((buffer[3] << 0) & 0x000000FFU));
        return val;
    }

    public static ulong ToUInt64BigEndian(byte[] buffer, int offset)
    {
        return ((ulong)ToUInt32BigEndian(buffer, offset + 0) << 32) | ToUInt32BigEndian(buffer, offset + 4);
    }

    public static ulong ToUInt64BigEndian(ReadOnlySpan<byte> buffer)
    {
        return ((ulong)ToUInt32BigEndian(buffer.Slice(0, 4)) << 32) | ToUInt32BigEndian(buffer.Slice(4, 4));
    }

    public static short ToInt16BigEndian(byte[] buffer, int offset)
    {
        return (short)ToUInt16BigEndian(buffer, offset);
    }

    public static short ToInt16BigEndian(ReadOnlySpan<byte> buffer)
    {
        return (short)ToUInt16BigEndian(buffer);
    }

    public static int ToInt32BigEndian(byte[] buffer, int offset)
    {
        return (int)ToUInt32BigEndian(buffer, offset);
    }

    public static int ToInt32BigEndian(ReadOnlySpan<byte> buffer)
    {
        return (int)ToUInt32BigEndian(buffer);
    }

    public static long ToInt64BigEndian(byte[] buffer, int offset)
    {
        return (long)ToUInt64BigEndian(buffer, offset);
    }

    public static long ToInt64BigEndian(ReadOnlySpan<byte> buffer)
    {
        return (long)ToUInt64BigEndian(buffer);
    }

    public static Guid ToGuidLittleEndian(byte[] buffer, int offset)
    {
        return MemoryMarshal.Read<Guid>(buffer.AsSpan(offset, 16));
    }

    public static Guid ToGuidLittleEndian(ReadOnlySpan<byte> buffer)
    {
        return MemoryMarshal.Read<Guid>(buffer.Slice(0, 16));
    }

    public static Guid ToGuidBigEndian(byte[] buffer, int offset)
    {
        return new Guid(
            ToUInt32BigEndian(buffer, offset + 0),
            ToUInt16BigEndian(buffer, offset + 4),
            ToUInt16BigEndian(buffer, offset + 6),
            buffer[offset + 8],
            buffer[offset + 9],
            buffer[offset + 10],
            buffer[offset + 11],
            buffer[offset + 12],
            buffer[offset + 13],
            buffer[offset + 14],
            buffer[offset + 15]);
    }

    public static Guid ToGuidBigEndian(ReadOnlySpan<byte> buffer)
    {
        return new Guid(
            ToUInt32BigEndian(buffer.Slice(0, 4)),
            ToUInt16BigEndian(buffer.Slice(4, 2)),
            ToUInt16BigEndian(buffer.Slice(6, 2)),
            buffer[8],
            buffer[9],
            buffer[10],
            buffer[11],
            buffer[12],
            buffer[13],
            buffer[14],
            buffer[15]);
    }

    public static byte[] ToByteArray(byte[] buffer, int offset, int length)
    {
        var result = new byte[length];
        Array.Copy(buffer, offset, result, 0, length);
        return result;
    }

    public static byte[] ToByteArray(ReadOnlySpan<byte> buffer)
    {
        var result = new byte[buffer.Length];
        buffer.CopyTo(result);
        return result;
    }

    public static T ToStruct<T>(byte[] buffer, int offset)
        where T : IByteArraySerializable, new()
    {
        var result = new T();
        result.ReadFrom(buffer, offset);
        return result;
    }

    /// <summary>
    /// Primitive conversion from Unicode to ASCII that preserves special characters.
    /// </summary>
    /// <param name="value">The string to convert.</param>
    /// <param name="dest">The buffer to fill.</param>
    /// <param name="offset">The start of the string in the buffer.</param>
    /// <param name="count">The number of characters to convert.</param>
    /// <remarks>The built-in ASCIIEncoding converts characters of codepoint > 127 to ?,
    /// this preserves those code points by removing the top 16 bits of each character.</remarks>
    public static void StringToBytes(string value, byte[] dest, int offset, int count)
    {
        Encoding.GetEncoding(28591).GetBytes(value, 0, Math.Min(count, value.Length), dest, offset);
    }

    /// <summary>
    /// Primitive conversion from Unicode to ASCII that preserves special characters.
    /// </summary>
    /// <param name="value">The string to convert.</param>
    /// <param name="dest">The buffer to fill.</param>
    /// <param name="offset">The start of the string in the buffer.</param>
    /// <param name="count">The number of characters to convert.</param>
    /// <remarks>The built-in ASCIIEncoding converts characters of codepoint > 127 to ?,
    /// this preserves those code points by removing the top 16 bits of each character.</remarks>
    public static void StringToBytes(string value, Span<byte> dest)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        Encoding.GetEncoding(28591).GetBytes(value.AsSpan(0, Math.Min(dest.Length, value.Length)), dest);
#else
        var buffer = ArrayPool<byte>.Shared.Rent(dest.Length);
        try
        {
            Array.Clear(buffer, 0, dest.Length);
            StringToBytes(value, buffer, 0, dest.Length);
            buffer.AsSpan(0, dest.Length).CopyTo(dest);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
#endif
    }

    /// <summary>
    /// Primitive conversion from ASCII to Unicode that preserves special characters.
    /// </summary>
    /// <param name="data">The data to convert.</param>
    /// <param name="offset">The first byte to convert.</param>
    /// <param name="count">The number of bytes to convert.</param>
    /// <returns>The string.</returns>
    /// <remarks>The built-in ASCIIEncoding converts characters of codepoint > 127 to ?,
    /// this preserves those code points.</remarks>
    public static string BytesToString(byte[] data, int offset, int count)
    {
        return Encoding.GetEncoding(28591).GetString(data, offset, count);
    }

    /// <summary>
    /// Primitive conversion from ASCII to Unicode that preserves special characters.
    /// </summary>
    /// <param name="data">The data to convert.</param>
    /// <param name="offset">The first byte to convert.</param>
    /// <param name="count">The number of bytes to convert.</param>
    /// <returns>The string.</returns>
    /// <remarks>The built-in ASCIIEncoding converts characters of codepoint > 127 to ?,
    /// this preserves those code points.</remarks>
    public static string BytesToString(ReadOnlySpan<byte> data)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        return Encoding.GetEncoding(28591).GetString(data);
#else
        var buffer = ArrayPool<byte>.Shared.Rent(data.Length);
        try
        {
            data.CopyTo(buffer);
            return Encoding.GetEncoding(28591).GetString(buffer, 0, data.Length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
#endif
    }

    /// <summary>
    /// Primitive conversion from ASCII to Unicode that stops at a null-terminator.
    /// </summary>
    /// <param name="data">The data to convert.</param>
    /// <param name="offset">The first byte to convert.</param>
    /// <param name="count">The number of bytes to convert.</param>
    /// <returns>The string.</returns>
    /// <remarks>The built-in ASCIIEncoding converts characters of codepoint > 127 to ?,
    /// this preserves those code points.</remarks>
    public static string BytesToZString(byte[] data, int offset, int count)
    {
        var z = Array.IndexOf(data, default, offset, Math.Min(count, data.Length - offset));

        if (z >= 0)
        {
            count = z - offset;
        }

        return Encoding.GetEncoding(28591).GetString(data, offset, count);
    }

    /// <summary>
    /// Primitive conversion from ASCII to Unicode that stops at a null-terminator.
    /// </summary>
    /// <param name="data">The data to convert.</param>
    /// <param name="offset">The first byte to convert.</param>
    /// <param name="count">The number of bytes to convert.</param>
    /// <returns>The string.</returns>
    /// <remarks>The built-in ASCIIEncoding converts characters of codepoint > 127 to ?,
    /// this preserves those code points.</remarks>
    public static string BytesToZString(ReadOnlySpan<byte> data)
    {
        var z = data.IndexOf(default(byte));

        if (z >= 0)
        {
            data = data.Slice(0, z);
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        return Encoding.GetEncoding(28591).GetString(data);
#else
        var buffer = ArrayPool<byte>.Shared.Rent(data.Length);
        try
        {
            data.CopyTo(buffer);
            return Encoding.GetEncoding(28591).GetString(buffer, 0, data.Length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
#endif
    }

#endregion
}
