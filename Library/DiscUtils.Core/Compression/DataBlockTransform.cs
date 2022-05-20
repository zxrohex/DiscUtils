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
using System.Globalization;

namespace DiscUtils.Compression;

internal abstract class DataBlockTransform
{
    protected abstract bool BuffersMustNotOverlap { get; }

    public int Process(byte[] input, int inputOffset, int inputCount, byte[] output, int outputOffset)
    {
        if (output.Length < outputOffset + (long)MinOutputCount(inputCount))
        {
            throw new ArgumentException(
                $"Output buffer to small, must be at least {MinOutputCount(inputCount)} bytes may need to be {MaxOutputCount(inputCount)} bytes");
        }

        if (BuffersMustNotOverlap)
        {
            var maxOut = MaxOutputCount(inputCount);

            if (input == output
                && (inputOffset + (long)inputCount > outputOffset)
                && (inputOffset <= outputOffset + (long)maxOut))
            {
                var tempBuffer = ArrayPool<byte>.Shared.Rent(maxOut);
                try
                {
                    var outCount = DoProcess(input, inputOffset, inputCount, tempBuffer, 0);
                    Array.Copy(tempBuffer, 0, output, outputOffset, outCount);

                    return outCount;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(tempBuffer);
                }
            }
        }

        return DoProcess(input, inputOffset, inputCount, output, outputOffset);
    }

    protected abstract int DoProcess(byte[] input, int inputOffset, int inputCount, byte[] output, int outputOffset);

    protected abstract int MaxOutputCount(int inputCount);

    protected abstract int MinOutputCount(int inputCount);
}