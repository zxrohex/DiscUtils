//
// Copyright (c) 2017, Bianco Veigel
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

namespace DiscUtils.Btrfs.Base;

internal class NodeItem : IByteArraySerializable
{
    public static readonly int Length = Key.Length + 0x8;
    
    public Key Key { get; set; }

    public uint DataOffset { get; set; }

    public uint DataSize { get; set; }
    
    public virtual int Size
    {
        get { return Length; }
    }

    public virtual int ReadFrom(byte[] buffer, int offset) => ReadFrom(buffer.AsSpan(offset));

    public virtual int ReadFrom(ReadOnlySpan<byte> buffer)
    {
        Key = new Key();
        var offset = Key.ReadFrom(buffer);
        DataOffset = EndianUtilities.ToUInt32LittleEndian(buffer.Slice(offset));
        DataSize = EndianUtilities.ToUInt32LittleEndian(buffer.Slice(offset + 0x4));
        return Size;
    }

    public void WriteTo(byte[] buffer, int offset)
    {
        throw new NotImplementedException();
    }

    public override string ToString()
    {
        return Key.ToString();
    }
}
