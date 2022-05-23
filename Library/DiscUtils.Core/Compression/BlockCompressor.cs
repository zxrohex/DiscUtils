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

namespace DiscUtils.Compression;

/// <summary>
/// Base class for block compression algorithms.
/// </summary>
public abstract class BlockCompressor
{
    /// <summary>
    /// Gets or sets the block size parameter to the algorithm.
    /// </summary>
    /// <remarks>
    /// Some algorithms may use this to control both compression and decompression, others may
    /// only use it to control compression.  Some may ignore it entirely.
    /// </remarks>
    public int BlockSize { get; set; }

    /// <summary>
    /// Compresses some data.
    /// </summary>
    /// <param name="source">The uncompressed input.</param>
    /// <param name="compressed">The destination for the output compressed data.</param>
    /// <param name="compressedLength">Actual size used of compressed buffer on output.</param>
    /// <returns>Indication of success, or indication the data could not compress into the requested space.</returns>
    public abstract CompressionResult Compress(ReadOnlySpan<byte> source, Span<byte> compressed,
                                               out int compressedLength);

    /// <summary>
    /// Decompresses some data.
    /// </summary>
    /// <param name="source">The compressed input.</param>
    /// <param name="decompressed">The destination for the output decompressed data.</param>
    /// <returns>The amount of decompressed data.</returns>
    public abstract int Decompress(ReadOnlySpan<byte> source, Span<byte> decompressed);
}