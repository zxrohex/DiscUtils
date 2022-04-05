//
// Copyright (c) 2008-2012, Kenneth Bell
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
using System.Threading;
using System.Threading.Tasks;

namespace DiscUtils.Streams
{
    /// <summary>
    /// Aligns I/O to a given block size.
    /// </summary>
    /// <remarks>Uses the read-modify-write pattern to align I/O.</remarks>
    public sealed class AligningStream : WrappingMappedStream<SparseStream>
    {
        private readonly byte[] _alignmentBuffer;
        private readonly int _blockSize;
        private long _position;

        public AligningStream(SparseStream toWrap, Ownership ownership, int blockSize)
            : base(toWrap, ownership, null)
        {
            _blockSize = blockSize;
            _alignmentBuffer = new byte[blockSize];
        }

        public override long Position
        {
            get { return _position; }
            set { _position = value; }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (count % _blockSize == 0 || _position + count == Length))
            {
                // Aligned read - pass through to underlying stream.
                WrappedStream.Position = _position;
                int numRead = WrappedStream.Read(buffer, offset, count);
                _position += numRead;
                return numRead;
            }

            long startPos = MathUtilities.RoundDown(_position, _blockSize);
            long endPos = MathUtilities.RoundUp(_position + count, _blockSize);

            if (endPos - startPos > int.MaxValue)
            {
                throw new IOException("Oversized read, after alignment");
            }

            byte[] tempBuffer = new byte[endPos - startPos];

            WrappedStream.Position = startPos;
            int read = WrappedStream.Read(tempBuffer, 0, tempBuffer.Length);
            int available = Math.Min(count, read - startOffset);

            Array.Copy(tempBuffer, startOffset, buffer, offset, available);

            _position += available;
            return available;
        }

#if NET45_OR_GREATER || NETSTANDARD || NETCOREAPP
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (count % _blockSize == 0 || _position + count == Length))
            {
                // Aligned read - pass through to underlying stream.
                WrappedStream.Position = _position;
                int numRead = await WrappedStream.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
                _position += numRead;
                return numRead;
            }

            long startPos = MathUtilities.RoundDown(_position, _blockSize);
            long endPos = MathUtilities.RoundUp(_position + count, _blockSize);

            if (endPos - startPos > int.MaxValue)
            {
                throw new IOException("Oversized read, after alignment");
            }

            byte[] tempBuffer = new byte[endPos - startPos];

            WrappedStream.Position = startPos;
            int read = await WrappedStream.ReadAsync(tempBuffer, 0, tempBuffer.Length, cancellationToken).ConfigureAwait(false);
            int available = Math.Min(count, read - startOffset);

            Array.Copy(tempBuffer, startOffset, buffer, offset, available);

            _position += available;
            return available;
        }
#endif

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        public override int Read(Span<byte> buffer)
        {
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (buffer.Length % _blockSize == 0 || _position + buffer.Length == Length))
            {
                // Aligned read - pass through to underlying stream.
                WrappedStream.Position = _position;
                int numRead = WrappedStream.Read(buffer);
                _position += numRead;
                return numRead;
            }

            long startPos = MathUtilities.RoundDown(_position, _blockSize);
            long endPos = MathUtilities.RoundUp(_position + buffer.Length, _blockSize);

            if (endPos - startPos > int.MaxValue)
            {
                throw new IOException("Oversized read, after alignment");
            }

            byte[] tempBuffer = new byte[endPos - startPos];

            WrappedStream.Position = startPos;
            int read = WrappedStream.Read(tempBuffer, 0, tempBuffer.Length);
            int available = Math.Min(buffer.Length, read - startOffset);

            tempBuffer.AsSpan(startOffset, available).CopyTo(buffer);

            _position += available;
            return available;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (buffer.Length % _blockSize == 0 || _position + buffer.Length == Length))
            {
                // Aligned read - pass through to underlying stream.
                WrappedStream.Position = _position;
                int numRead = await WrappedStream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
                _position += numRead;
                return numRead;
            }

            long startPos = MathUtilities.RoundDown(_position, _blockSize);
            long endPos = MathUtilities.RoundUp(_position + buffer.Length, _blockSize);

            if (endPos - startPos > int.MaxValue)
            {
                throw new IOException("Oversized read, after alignment");
            }

            byte[] tempBuffer = new byte[endPos - startPos];

            WrappedStream.Position = startPos;
            int read = await WrappedStream.ReadAsync(tempBuffer, 0, tempBuffer.Length, cancellationToken).ConfigureAwait(false);
            int available = Math.Min(buffer.Length, read - startOffset);

            tempBuffer.AsMemory(startOffset, available).CopyTo(buffer);

            _position += available;
            return available;
        }
#endif

        public override long Seek(long offset, SeekOrigin origin)
        {
            long effectiveOffset = offset;
            if (origin == SeekOrigin.Current)
            {
                effectiveOffset += _position;
            }
            else if (origin == SeekOrigin.End)
            {
                effectiveOffset += Length;
            }

            if (effectiveOffset < 0)
            {
                throw new IOException("Attempt to move before beginning of stream");
            }
            _position = effectiveOffset;
            return _position;
        }

        public override void Clear(int count)
        {
            DoOperation(
                (s, opOffset, opCount) => { s.Clear(opCount); },
                (buffer, offset, opOffset, opCount) => { Array.Clear(buffer, offset, opCount); },
                count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            DoOperation(
                (s, opOffset, opCount) => { s.Write(buffer, offset + opOffset, opCount); },
                (tempBuffer, tempOffset, opOffset, opCount) => { Array.Copy(buffer, offset + opOffset, tempBuffer, tempOffset, opCount); },
                count);
        }

#if NET45_OR_GREATER || NETSTANDARD || NETCOREAPP
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (count % _blockSize == 0 || _position + count == Length))
            {
                WrappedStream.Position = _position;
                await WrappedStream.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
                _position += count;
                return;
            }

            long unalignedEnd = _position + count;
            long alignedPos = MathUtilities.RoundDown(_position, _blockSize);

            if (startOffset != 0)
            {
                WrappedStream.Position = alignedPos;
                await WrappedStream.ReadAsync(_alignmentBuffer, 0, _blockSize, cancellationToken).ConfigureAwait(false);

                Array.Copy(buffer, offset, _alignmentBuffer, startOffset, Math.Min(count, _blockSize - startOffset));

                WrappedStream.Position = alignedPos;
                await WrappedStream.WriteAsync(_alignmentBuffer, 0, _blockSize, cancellationToken).ConfigureAwait(false);
            }

            alignedPos = MathUtilities.RoundUp(_position, _blockSize);
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            int passthroughLength = (int)MathUtilities.RoundDown(_position + count - alignedPos, _blockSize);
            if (passthroughLength > 0)
            {
                WrappedStream.Position = alignedPos;
                await WrappedStream.WriteAsync(buffer, offset + (int)(alignedPos - _position), passthroughLength, cancellationToken).ConfigureAwait(false);
            }

            alignedPos += passthroughLength;
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            WrappedStream.Position = alignedPos;
            await WrappedStream.ReadAsync(_alignmentBuffer, 0, _blockSize, cancellationToken).ConfigureAwait(false);

            Array.Copy(buffer, offset + (int)(alignedPos - _position), _alignmentBuffer, 0, (int)Math.Min(count - (alignedPos - _position), unalignedEnd - alignedPos));

            WrappedStream.Position = alignedPos;
            await WrappedStream.WriteAsync(_alignmentBuffer, 0, _blockSize, cancellationToken).ConfigureAwait(false);

            _position = unalignedEnd;
        }
#endif

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            var count = buffer.Length;
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (count % _blockSize == 0 || _position + count == Length))
            {
                WrappedStream.Position = _position;
                await WrappedStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
                _position += count;
                return;
            }

            long unalignedEnd = _position + count;
            long alignedPos = MathUtilities.RoundDown(_position, _blockSize);

            if (startOffset != 0)
            {
                WrappedStream.Position = alignedPos;
                await WrappedStream.ReadAsync(_alignmentBuffer.AsMemory(0, _blockSize), cancellationToken).ConfigureAwait(false);

                buffer[..Math.Min(count, _blockSize - startOffset)].CopyTo(_alignmentBuffer.AsMemory(startOffset));

                WrappedStream.Position = alignedPos;
                await WrappedStream.WriteAsync(_alignmentBuffer.AsMemory(0, _blockSize), cancellationToken).ConfigureAwait(false);
            }

            alignedPos = MathUtilities.RoundUp(_position, _blockSize);
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            int passthroughLength = (int)MathUtilities.RoundDown(_position + count - alignedPos, _blockSize);
            if (passthroughLength > 0)
            {
                WrappedStream.Position = alignedPos;
                await WrappedStream.WriteAsync(buffer.Slice((int)(alignedPos - _position), passthroughLength), cancellationToken).ConfigureAwait(false);
            }

            alignedPos += passthroughLength;
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            WrappedStream.Position = alignedPos;
            await WrappedStream.ReadAsync(_alignmentBuffer.AsMemory(0, _blockSize), cancellationToken).ConfigureAwait(false);

            buffer.Slice((int)(alignedPos - _position), (int)Math.Min(count - (alignedPos - _position), unalignedEnd - alignedPos)).CopyTo(_alignmentBuffer);

            WrappedStream.Position = alignedPos;
            await WrappedStream.WriteAsync(_alignmentBuffer.AsMemory(0, _blockSize), cancellationToken).ConfigureAwait(false);

            _position = unalignedEnd;
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            var count = buffer.Length;
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (count % _blockSize == 0 || _position + count == Length))
            {
                WrappedStream.Position = _position;
                WrappedStream.Write(buffer);
                _position += count;
                return;
            }

            long unalignedEnd = _position + count;
            long alignedPos = MathUtilities.RoundDown(_position, _blockSize);

            if (startOffset != 0)
            {
                WrappedStream.Position = alignedPos;
                WrappedStream.Read(_alignmentBuffer.AsSpan(0, _blockSize));

                buffer[..Math.Min(count, _blockSize - startOffset)].CopyTo(_alignmentBuffer.AsSpan(startOffset));

                WrappedStream.Position = alignedPos;
                WrappedStream.Write(_alignmentBuffer.AsSpan(0, _blockSize));
            }

            alignedPos = MathUtilities.RoundUp(_position, _blockSize);
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            int passthroughLength = (int)MathUtilities.RoundDown(_position + count - alignedPos, _blockSize);
            if (passthroughLength > 0)
            {
                WrappedStream.Position = alignedPos;
                WrappedStream.Write(buffer.Slice((int)(alignedPos - _position), passthroughLength));
            }

            alignedPos += passthroughLength;
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            WrappedStream.Position = alignedPos;
            WrappedStream.Read(_alignmentBuffer.AsSpan(0, _blockSize));

            buffer.Slice((int)(alignedPos - _position), (int)Math.Min(count - (alignedPos - _position), unalignedEnd - alignedPos)).CopyTo(_alignmentBuffer);

            WrappedStream.Position = alignedPos;
            WrappedStream.Write(_alignmentBuffer.AsSpan(0, _blockSize));

            _position = unalignedEnd;
        }
#endif

        private void DoOperation(ModifyStream modifyStream, ModifyBuffer modifyBuffer, int count)
        {
            int startOffset = (int)(_position % _blockSize);
            if (startOffset == 0 && (count % _blockSize == 0 || _position + count == Length))
            {
                WrappedStream.Position = _position;
                modifyStream(WrappedStream, 0, count);
                _position += count;
                return;
            }

            long unalignedEnd = _position + count;
            long alignedPos = MathUtilities.RoundDown(_position, _blockSize);

            if (startOffset != 0)
            {
                WrappedStream.Position = alignedPos;
                WrappedStream.Read(_alignmentBuffer, 0, _blockSize);

                modifyBuffer(_alignmentBuffer, startOffset, 0, Math.Min(count, _blockSize - startOffset));

                WrappedStream.Position = alignedPos;
                WrappedStream.Write(_alignmentBuffer, 0, _blockSize);
            }

            alignedPos = MathUtilities.RoundUp(_position, _blockSize);
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            int passthroughLength = (int)MathUtilities.RoundDown(_position + count - alignedPos, _blockSize);
            if (passthroughLength > 0)
            {
                WrappedStream.Position = alignedPos;
                modifyStream(WrappedStream, (int)(alignedPos - _position), passthroughLength);
            }

            alignedPos += passthroughLength;
            if (alignedPos >= unalignedEnd)
            {
                _position = unalignedEnd;
                return;
            }

            WrappedStream.Position = alignedPos;
            WrappedStream.Read(_alignmentBuffer, 0, _blockSize);

            modifyBuffer(_alignmentBuffer, 0, (int)(alignedPos - _position), (int)Math.Min(count - (alignedPos - _position), unalignedEnd - alignedPos));

            WrappedStream.Position = alignedPos;
            WrappedStream.Write(_alignmentBuffer, 0, _blockSize);

            _position = unalignedEnd;
        }

        private delegate void ModifyStream(SparseStream stream, int opOffset, int count);

        private delegate void ModifyBuffer(byte[] buffer, int offset, int opOffset, int count);
    }
}