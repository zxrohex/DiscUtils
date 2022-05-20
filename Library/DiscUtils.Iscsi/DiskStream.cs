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
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DiscUtils.Streams;

namespace DiscUtils.Iscsi;

internal class DiskStream : SparseStream
{
    private readonly int _blockSize;

    private readonly long _length;
    private readonly long _lun;
    private long _position;
    private readonly Session _session;

    public DiskStream(Session session, long lun, FileAccess access)
    {
        _session = session;
        _lun = lun;

        var capacity = session.GetCapacity(lun);
        _blockSize = capacity.BlockSize;
        _length = capacity.LogicalBlockCount * capacity.BlockSize;
        CanWrite = access != FileAccess.Read;
        CanRead = access != FileAccess.Write;
    }

    public override bool CanRead { get; }

    public override bool CanSeek
    {
        get { return true; }
    }

    public override bool CanWrite { get; }

    public override IEnumerable<StreamExtent> Extents
    {
        get { yield return new StreamExtent(0, _length); }
    }

    public override long Length
    {
        get { return _length; }
    }

    public override long Position
    {
        get { return _position; }

        set { _position = value; }
    }

    public override void Flush() {}

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (!CanRead)
        {
            throw new InvalidOperationException("Attempt to read from write-only stream");
        }

        var maxToRead = (int)Math.Min(_length - _position, count);

        var firstBlock = _position / _blockSize;
        var lastBlock = MathUtilities.Ceil(_position + maxToRead, _blockSize);

        var tempBuffer = ArrayPool<byte>.Shared.Rent(checked((int)((lastBlock - firstBlock) * _blockSize)));
        try
        {
            var numRead = _session.Read(_lun, firstBlock, (short)(lastBlock - firstBlock), tempBuffer, 0);

            var numCopied = Math.Min(maxToRead, numRead);
            Array.Copy(tempBuffer, (int)(_position - firstBlock * _blockSize), buffer, offset, numCopied);

            _position += numCopied;

            return numCopied;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (!CanRead)
        {
            throw new InvalidOperationException("Attempt to read from write-only stream");
        }

        var maxToRead = (int)Math.Min(_length - _position, count);

        var firstBlock = _position / _blockSize;
        var lastBlock = MathUtilities.Ceil(_position + maxToRead, _blockSize);

        var tempBuffer = ArrayPool<byte>.Shared.Rent(checked((int)((lastBlock - firstBlock) * _blockSize)));
        try
        {
            var numRead = await _session.ReadAsync(_lun, firstBlock, (short)(lastBlock - firstBlock), tempBuffer, 0, cancellationToken).ConfigureAwait(false);

            var numCopied = Math.Min(maxToRead, numRead);
            Array.Copy(tempBuffer, (int)(_position - firstBlock * _blockSize), buffer, offset, numCopied);

            _position += numCopied;

            return numCopied;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override int Read(Span<byte> buffer)
    {
        if (!CanRead)
        {
            throw new InvalidOperationException("Attempt to read from read-only stream");
        }

        var maxToRead = (int)Math.Min(_length - _position, buffer.Length);

        var firstBlock = _position / _blockSize;
        var lastBlock = MathUtilities.Ceil(_position + maxToRead, _blockSize);

        var tempBuffer = ArrayPool<byte>.Shared.Rent(checked((int)((lastBlock - firstBlock) * _blockSize)));
        try
        {
            var numRead = _session.Read(_lun, firstBlock, (short)(lastBlock - firstBlock), tempBuffer, 0);

            var numCopied = Math.Min(maxToRead, numRead);
            tempBuffer.AsSpan((int)(_position - firstBlock * _blockSize), numCopied).CopyTo(buffer);

            _position += numCopied;

            return numCopied;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (!CanRead)
        {
            throw new InvalidOperationException("Attempt to read from read-only stream");
        }

        var maxToRead = (int)Math.Min(_length - _position, buffer.Length);

        var firstBlock = _position / _blockSize;
        var lastBlock = MathUtilities.Ceil(_position + maxToRead, _blockSize);

        var tempBuffer = ArrayPool<byte>.Shared.Rent(checked((int)((lastBlock - firstBlock) * _blockSize)));
        try
        {
            var numRead = await _session.ReadAsync(_lun, firstBlock, (short)(lastBlock - firstBlock), tempBuffer, 0, cancellationToken).ConfigureAwait(false);

            var numCopied = Math.Min(maxToRead, numRead);
            tempBuffer.AsSpan((int)(_position - firstBlock * _blockSize), numCopied).CopyTo(buffer.Span);

            _position += numCopied;

            return numCopied;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
        }
    }
#endif

    public override long Seek(long offset, SeekOrigin origin)
    {
        var effectiveOffset = offset;
        if (origin == SeekOrigin.Current)
        {
            effectiveOffset += _position;
        }
        else if (origin == SeekOrigin.End)
        {
            effectiveOffset += _length;
        }

        if (effectiveOffset < 0)
        {
            throw new IOException("Attempt to move before beginning of disk");
        }
        _position = effectiveOffset;
        return _position;
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (!CanWrite)
        {
            throw new IOException("Attempt to write to read-only stream");
        }

        if (_position + count > _length)
        {
            throw new IOException("Attempt to write beyond end of stream");
        }

        var numWritten = 0;

        while (numWritten < count)
        {
            var block = _position / _blockSize;
            var offsetInBlock = (uint)(_position % _blockSize);

            var toWrite = count - numWritten;

            // Need to read - we're not handling a full block
            if (offsetInBlock != 0 || toWrite < _blockSize)
            {
                toWrite = (int)Math.Min(toWrite, _blockSize - offsetInBlock);

                var blockBuffer = new byte[_blockSize];
                var numRead = _session.Read(_lun, block, 1, blockBuffer, 0);

                if (numRead != _blockSize)
                {
                    throw new IOException("Incomplete read, received " + numRead + " bytes from 1 block");
                }

                // Overlay as much data as we have for this block
                Array.Copy(buffer, offset + numWritten, blockBuffer, (int)offsetInBlock, toWrite);

                // Write the block back
                _session.Write(_lun, block, 1, _blockSize, blockBuffer, 0);
            }
            else
            {
                // Processing at least one whole block, just write (after making sure to trim any partial sectors from the end)...
                var numBlocks = (short)(toWrite / _blockSize);
                toWrite = numBlocks * _blockSize;

                _session.Write(_lun, block, numBlocks, _blockSize, buffer, offset + numWritten);
            }

            numWritten += toWrite;
            _position += toWrite;
        }
    }
}