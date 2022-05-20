using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace DiscUtils.CoreCompat;

internal static class StringExtensions
{
#if !NETSTANDARD2_1_OR_GREATER && !NETCOREAPP
    public static string[] Split(this string str, char separator, int count, StringSplitOptions options = StringSplitOptions.None) =>
        str.Split(new[] { separator }, count, options);

    public static string[] Split(this string str, char separator, StringSplitOptions options = StringSplitOptions.None) =>
        str.Split(new[] { separator }, options);

    public static void NextBytes(this Random random, Span<byte> buffer)
    {
        var bytes = new byte[buffer.Length];
        random.NextBytes(bytes);
        bytes.AsSpan().CopyTo(buffer);
    }

    public static int Read(this Stream stream, Span<byte> buffer)
    {
        var bytes = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            var numRead = stream.Read(bytes, 0, buffer.Length);
            bytes.AsSpan(0, numRead).CopyTo(buffer);
            return numRead;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    public static void Write(this Stream stream, ReadOnlySpan<byte> buffer)
    {
        var bytes = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            buffer.CopyTo(bytes);
            stream.Write(bytes, 0, buffer.Length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }
#endif
}

