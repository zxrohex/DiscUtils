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
using System.Threading;
using System.Threading.Tasks;

namespace DiscUtils.Streams;

public class BuilderBytesExtent : BuilderExtent
{
    protected byte[] _data;

    public BuilderBytesExtent(long start, byte[] data)
        : base(start, data.Length)
    {
        _data = data;
    }

    protected BuilderBytesExtent(long start, long length)
        : base(start, length) {}

    protected override void Dispose(bool disposing) {}

    public override void PrepareForRead() {}

    public override int Read(long diskOffset, byte[] block, int offset, int count)
    {
        var start = (int)Math.Min(diskOffset - Start, _data.Length);
        var numRead = Math.Min(count, _data.Length - start);

        System.Buffer.BlockCopy(_data, start, block, offset, numRead);

        return numRead;
    }

    public override ValueTask<int> ReadAsync(long diskOffset, byte[] block, int offset, int count, CancellationToken cancellationToken)
    {
        var start = (int)Math.Min(diskOffset - Start, _data.Length);
        var numRead = Math.Min(count, _data.Length - start);

        System.Buffer.BlockCopy(_data, start, block, offset, numRead);

        return new(numRead);
    }

    public override ValueTask<int> ReadAsync(long diskOffset, Memory<byte> block, CancellationToken cancellationToken)
    {
        var start = (int)Math.Min(diskOffset - Start, _data.Length);
        var numRead = Math.Min(block.Length, _data.Length - start);

        _data.AsMemory(start, numRead).CopyTo(block);

        return new(numRead);
    }

    public override int Read(long diskOffset, Span<byte> block)
    {
        var start = (int)Math.Min(diskOffset - Start, _data.Length);
        var numRead = Math.Min(block.Length, _data.Length - start);

        _data.AsSpan(start, numRead).CopyTo(block);

        return numRead;
    }

    public override void DisposeReadState() {}
}