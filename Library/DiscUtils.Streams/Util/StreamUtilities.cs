﻿//
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

using DiscUtils.Streams.Compatibility;
using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DiscUtils.Streams;

public static class StreamUtilities
{
    /// <summary>
    /// Validates standard buffer, offset, count parameters to a method.
    /// </summary>
    /// <param name="buffer">The byte array to read from / write to.</param>
    /// <param name="offset">The starting offset in <c>buffer</c>.</param>
    /// <param name="count">The number of bytes to read / write.</param>
    public static void AssertBufferParameters(byte[] buffer, int offset, int count)
    {
        if (buffer is null)
        {
            throw new ArgumentNullException(nameof(buffer));
        }

        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset is negative");
        }

        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "Count is negative");
        }

        if (buffer.Length < offset + count)
        {
            throw new ArgumentException("buffer is too small", nameof(buffer));
        }
    }

    #region Stream Manipulation

#if !NET7_0_OR_GREATER

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="buffer">The buffer to populate.</param>
    /// <param name="offset">Offset in the buffer to start.</param>
    /// <param name="count">The number of bytes to read.</param>
    public static void ReadExactly(this Stream stream, byte[] buffer, int offset, int count)
    {
        var originalCount = count;

        while (count > 0)
        {
            var numRead = stream.Read(buffer, offset, count);

            if (numRead == 0)
            {
                throw new EndOfStreamException($"Unable to complete read of {originalCount} bytes");
            }

            offset += numRead;
            count -= numRead;
        }
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="buffer">The buffer to populate.</param>
    /// <param name="cancellationToken"></param>
    public static async ValueTask ReadExactlyAsync(this Stream stream, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var originalCount = buffer.Length;

        while (buffer.Length > 0)
        {
            var numRead = await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);

            if (numRead == 0)
            {
                throw new EndOfStreamException($"Unable to complete read of {originalCount} bytes");
            }

            buffer = buffer.Slice(numRead);
        }
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="buffer">The buffer to populate.</param>
    public static void ReadExactly(this Stream stream, Span<byte> buffer)
    {
        var originalCount = buffer.Length;

        while (buffer.Length > 0)
        {
            var numRead = stream.Read(buffer);

            if (numRead == 0)
            {
                throw new EndOfStreamException($"Unable to complete read of {originalCount} bytes");
            }

            buffer = buffer.Slice(numRead);
        }
    }

#endif

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="count">The number of bytes to read.</param>
    /// <returns>The data read from the stream.</returns>
    public static byte[] ReadExactly(this Stream stream, int count)
    {
        var buffer = new byte[count];

        stream.ReadExactly(buffer, 0, count);

        return buffer;
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="count">The number of bytes to read.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The data read from the stream.</returns>
    public static async ValueTask<byte[]> ReadExactlyAsync(this Stream stream, int count, CancellationToken cancellationToken)
    {
        var buffer = new byte[count];

        await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);

        return buffer;
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The stream to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="data">The buffer to populate.</param>
    /// <param name="offset">Offset in the buffer to start.</param>
    /// <param name="count">The number of bytes to read.</param>
    public static void ReadExactly(this IBuffer buffer, long pos, byte[] data, int offset, int count)
    {
        var originalCount = count;

        while (count > 0)
        {
            var numRead = buffer.Read(pos, data, offset, count);

            if (numRead == 0)
            {
                throw new EndOfStreamException($"Unable to complete read of {originalCount} bytes");
            }

            pos += numRead;
            offset += numRead;
            count -= numRead;
        }
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The stream to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="data">The buffer to populate.</param>
    /// <param name="cancellationToken"></param>
    public static async ValueTask ReadExactlyAsync(this IBuffer buffer, long pos, Memory<byte> data, CancellationToken cancellationToken)
    {
        var originalCount = data.Length;

        while (data.Length > 0)
        {
            var numRead = await buffer.ReadAsync(pos, data, cancellationToken).ConfigureAwait(false);

            if (numRead == 0)
            {
                throw new EndOfStreamException($"Unable to complete read of {originalCount} bytes");
            }

            pos += numRead;
            data = data.Slice(numRead);
        }
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The stream to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="data">The buffer to populate.</param>
    public static void ReadExactly(this IBuffer buffer, long pos, Span<byte> data)
    {
        var originalCount = data.Length;

        while (data.Length > 0)
        {
            var numRead = buffer.Read(pos, data);

            if (numRead == 0)
            {
                throw new EndOfStreamException($"Unable to complete read of {originalCount} bytes");
            }

            pos += numRead;
            data = data.Slice(numRead);
        }
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The buffer to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="count">The number of bytes to read.</param>
    /// <returns>The data read from the stream.</returns>
    public static byte[] ReadExactly(this IBuffer buffer, long pos, int count)
    {
        var result = new byte[count];

        ReadExactly(buffer, pos, result, 0, count);

        return result;
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The buffer to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="count">The number of bytes to read.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The data read from the stream.</returns>
    public static async ValueTask<byte[]> ReadExactlyAsync(this IBuffer buffer, long pos, int count, CancellationToken cancellationToken)
    {
        var result = new byte[count];

        await ReadExactlyAsync(buffer, pos, result, cancellationToken).ConfigureAwait(false);

        return result;
    }

    /// <summary>
    /// Read bytes until buffer filled or EOF.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="buffer">The buffer to populate.</param>
    /// <param name="offset">Offset in the buffer to start.</param>
    /// <param name="count">The number of bytes to read.</param>
    /// <returns>The number of bytes actually read.</returns>
#if NET7_0_OR_GREATER
    public static int ReadMaximum(this Stream stream, byte[] buffer, int offset, int count)
        => stream.ReadAtLeast(buffer.AsSpan(offset, count), count, throwOnEndOfStream: false);
#else
    public static int ReadMaximum(this Stream stream, byte[] buffer, int offset, int count)
    {
        var totalRead = 0;

        while (count > 0)
        {
            var numRead = stream.Read(buffer, offset, count);

            if (numRead == 0)
            {
                return totalRead;
            }

            offset += numRead;
            count -= numRead;
            totalRead += numRead;
        }

        return totalRead;
    }
#endif

    /// <summary>
    /// Read bytes until buffer filled or EOF.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="buffer">The buffer to populate.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The number of bytes actually read.</returns>
#if NET7_0_OR_GREATER
    public static ValueTask<int> ReadMaximumAsync(this Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
        => stream.ReadAtLeastAsync(buffer, buffer.Length, throwOnEndOfStream: false, cancellationToken);
#else
    public static async ValueTask<int> ReadMaximumAsync(this Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;

        while (buffer.Length > 0)
        {
            var numRead = await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);

            if (numRead == 0)
            {
                return totalRead;
            }

            buffer = buffer.Slice(numRead);
            totalRead += numRead;
        }

        return totalRead;
    }
#endif

    /// <summary>
    /// Read bytes until buffer filled or EOF.
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="buffer">The buffer to populate.</param>
    /// <returns>The number of bytes actually read.</returns>
#if NET7_0_OR_GREATER
    public static int ReadMaximum(this Stream stream, Span<byte> buffer)
        => stream.ReadAtLeast(buffer, buffer.Length, throwOnEndOfStream: false);
#else
    public static int ReadMaximum(this Stream stream, Span<byte> buffer)
    {
        var totalRead = 0;

        while (buffer.Length > 0)
        {
            var numRead = stream.Read(buffer);

            if (numRead == 0)
            {
                return totalRead;
            }

            buffer = buffer.Slice(numRead);
            totalRead += numRead;
        }

        return totalRead;
    }
#endif

    /// <summary>
    /// Read bytes until buffer filled or EOF.
    /// </summary>
    /// <param name="buffer">The stream to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="data">The buffer to populate.</param>
    /// <param name="offset">Offset in the buffer to start.</param>
    /// <param name="count">The number of bytes to read.</param>
    /// <returns>The number of bytes actually read.</returns>
    public static int ReadMaximum(this IBuffer buffer, long pos, byte[] data, int offset, int count)
    {
        var totalRead = 0;

        while (count > 0)
        {
            var numRead = buffer.Read(pos, data, offset, count);

            if (numRead == 0)
            {
                return totalRead;
            }

            pos += numRead;
            offset += numRead;
            count -= numRead;
            totalRead += numRead;
        }

        return totalRead;
    }

    /// <summary>
    /// Read bytes until buffer filled or EOF.
    /// </summary>
    /// <param name="buffer">The stream to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="data">The buffer to populate.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The number of bytes actually read.</returns>
    public static async ValueTask<int> ReadMaximumAsync(this IBuffer buffer, long pos, Memory<byte> data, CancellationToken cancellationToken)
    {
        var totalRead = 0;

        while (data.Length > 0)
        {
            var numRead = await buffer.ReadAsync(pos, data, cancellationToken).ConfigureAwait(false);

            if (numRead == 0)
            {
                return totalRead;
            }

            pos += numRead;
            data = data.Slice(numRead);
            totalRead += numRead;
        }

        return totalRead;
    }

    /// <summary>
    /// Read bytes until buffer filled or EOF.
    /// </summary>
    /// <param name="buffer">The stream to read.</param>
    /// <param name="pos">The position in buffer to read from.</param>
    /// <param name="data">The buffer to populate.</param>
    /// <returns>The number of bytes actually read.</returns>
    public static int ReadMaximum(this IBuffer buffer, long pos, Span<byte> data)
    {
        var totalRead = 0;

        while (data.Length > 0)
        {
            var numRead = buffer.Read(pos, data);

            if (numRead == 0)
            {
                return totalRead;
            }

            pos += numRead;
            data = data.Slice(numRead);
            totalRead += numRead;
        }

        return totalRead;
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The buffer to read.</param>
    /// <returns>The data read from the stream.</returns>
    public static byte[] ReadAll(this IBuffer buffer)
    {
        return ReadExactly(buffer, 0, (int)buffer.Capacity);
    }

    /// <summary>
    /// Read bytes until buffer filled or throw EndOfStreamException.
    /// </summary>
    /// <param name="buffer">The buffer to read.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The data read from the stream.</returns>
    public static ValueTask<byte[]> ReadAllAsync(this IBuffer buffer, CancellationToken cancellationToken)
    {
        return ReadExactlyAsync(buffer, 0, (int)buffer.Capacity, cancellationToken);
    }

    /// <summary>
    /// Reads a disk sector (512 bytes).
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <returns>The sector data as a byte array.</returns>
    public static byte[] ReadSector(this Stream stream)
    {
        return ReadExactly(stream, Sizes.Sector);
    }

    /// <summary>
    /// Reads a disk sector (512 bytes).
    /// </summary>
    /// <param name="stream">The stream to read.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The sector data as a byte array.</returns>
    public static ValueTask<byte[]> ReadSectorAsync(this Stream stream, CancellationToken cancellationToken)
    {
        return ReadExactlyAsync(stream, Sizes.Sector, cancellationToken);
    }

    /// <summary>
    /// Reads a structure from a stream.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="stream">The stream to read.</param>
    /// <returns>The structure.</returns>
    public static T ReadStruct<T>(this Stream stream)
        where T : IByteArraySerializable, new()
    {
        var result = new T();

        if (result.Size <= 1024)
        {
            Span<byte> buffer = stackalloc byte[result.Size];
            stream.ReadExactly(buffer);
            result.ReadFrom(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(result.Size);
            try
            {
                stream.ReadExactly(buffer, 0, result.Size);
                result.ReadFrom(buffer.AsSpan(0, result.Size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        return result;
    }

    /// <summary>
    /// Reads a structure from a stream.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="stream">The stream to read.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The structure.</returns>
    public static async ValueTask<T> ReadStructAsync<T>(this Stream stream, CancellationToken cancellationToken)
        where T : IByteArraySerializable, new()
    {
        var result = new T();
        var buffer = ArrayPool<byte>.Shared.Rent(result.Size);
        try
        {
            var mem = buffer.AsMemory(0, result.Size);
            await stream.ReadExactlyAsync(mem, cancellationToken).ConfigureAwait(false);
            result.ReadFrom(mem.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return result;
    }

    /// <summary>
    /// Reads a structure from a stream.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="stream">The stream to read.</param>
    /// <param name="length">Number of bytes to read</param>
    /// <param name="cancellationToken"></param>
    /// <returns>The structure.</returns>
    public static async ValueTask<T> ReadStructAsync<T>(this Stream stream, int length, CancellationToken cancellationToken)
        where T : IByteArraySerializable, new()
    {
        var result = new T();
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var mem = buffer.AsMemory(0, length);
            await stream.ReadExactlyAsync(mem, cancellationToken).ConfigureAwait(false);
            result.ReadFrom(mem.Span);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return result;
    }

    /// <summary>
    /// Reads a structure from a stream into an existing instance.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="result">Existing instance</param>
    /// <param name="stream">The stream to read.</param>
    /// <returns>The structure.</returns>
    public static void ReadFrom<T>(this T result, Stream stream)
        where T : class, IByteArraySerializable
    {
        if (result.Size <= 1024)
        {
            Span<byte> buffer = stackalloc byte[result.Size];
            stream.ReadExactly(buffer);
            result.ReadFrom(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(result.Size);
            try
            {
                stream.ReadExactly(buffer, 0, result.Size);
                result.ReadFrom(buffer.AsSpan(0, result.Size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Reads a structure from a stream into an existing instance.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="result">Existing instance</param>
    /// <param name="stream">The stream to read.</param>
    /// <param name="length">Number of bytes to read from stream</param>
    /// <returns>The structure.</returns>
    public static void ReadFrom<T>(this T result, Stream stream, int length)
        where T : class, IByteArraySerializable
    {
        if (length <= 1024)
        {
            Span<byte> buffer = stackalloc byte[length];
            stream.ReadExactly(buffer);
            result.ReadFrom(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                stream.ReadExactly(buffer, 0, length);
                result.ReadFrom(buffer.AsSpan(0, length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Reads a structure from a stream into an existing instance.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="result">Existing instance</param>
    /// <param name="stream">The stream to read.</param>
    /// <returns>The structure.</returns>
    public static void ReadFrom<T>(this ref T result, Stream stream)
        where T : struct, IByteArraySerializable
    {
        if (result.Size <= 1024)
        {
            Span<byte> buffer = stackalloc byte[result.Size];
            stream.ReadExactly(buffer);
            result.ReadFrom(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(result.Size);
            try
            {
                stream.ReadExactly(buffer, 0, result.Size);
                result.ReadFrom(buffer.AsSpan(0, result.Size));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Reads a structure from a stream into an existing instance.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="result">Existing instance</param>
    /// <param name="stream">The stream to read.</param>
    /// <param name="length">Number of bytes to read from stream</param>
    /// <returns>The structure.</returns>
    public static void ReadFrom<T>(this ref T result, Stream stream, int length)
        where T : struct, IByteArraySerializable
    {
        if (length <= 1024)
        {
            Span<byte> buffer = stackalloc byte[length];
            stream.ReadExactly(buffer);
            result.ReadFrom(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                stream.ReadExactly(buffer, 0, length);
                result.ReadFrom(buffer.AsSpan(0, length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Reads a structure from a stream.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="stream">The stream to read.</param>
    /// <param name="length">The number of bytes to read.</param>
    /// <returns>The structure.</returns>
    public static T ReadStruct<T>(this Stream stream, int length)
        where T : IByteArraySerializable, new()
    {
        if (length <= 1024)
        {
            Span<byte> buffer = stackalloc byte[length];
            stream.ReadExactly(buffer);
            var result = new T();
            result.ReadFrom(buffer);
            return result;
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                stream.ReadExactly(buffer, 0, length);
                var result = new T();
                result.ReadFrom(buffer.AsSpan(0, length));
                return result;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Writes a structure to a stream.
    /// </summary>
    /// <typeparam name="T">The type of the structure.</typeparam>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="obj">The structure to write.</param>
    public static void WriteStruct<T>(this Stream stream, T obj)
        where T : IByteArraySerializable
    {
        if (obj.Size <= 1024)
        {
            Span<byte> buffer = stackalloc byte[obj.Size];
            obj.WriteTo(buffer);
            stream.Write(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(obj.Size);
            try
            {
                var span = buffer.AsSpan(0, obj.Size);
                obj.WriteTo(span);
                stream.Write(buffer, 0, span.Length);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

#endregion
}
