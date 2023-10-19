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

using DiscUtils.Streams.Compatibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DiscUtils.Streams;

/// <summary>
/// Base class for objects that can dynamically construct a stream.
/// </summary>
public abstract class StreamBuilder
{
    /// <summary>
    /// Builds a new stream.
    /// </summary>
    /// <returns>The stream created by the StreamBuilder instance.</returns>
    public virtual Stream Build()
    {
        var extents = FixExtents(out var totalLength);
        return new BuiltStream(totalLength, extents);
    }

    /// <summary>
    /// Builds a new stream.
    /// </summary>
    /// <returns>The stream created by the StreamBuilder instance.</returns>
    public virtual Task<Stream> BuildAsync(CancellationToken cancellationToken)
    {
        var extents = FixExtents(out var totalLength);
        return Task.FromResult((Stream)new BuiltStream(totalLength, extents));
    }

    /// <summary>
    /// Writes the stream contents to an existing stream.
    /// </summary>
    /// <param name="output">The stream to write to.</param>
    public virtual void Build(Stream output)
    {
        using var src = Build();
        var buffer = new byte[64 * 1024];
        var numRead = src.Read(buffer, 0, buffer.Length);
        while (numRead != 0)
        {
            output.Write(buffer, 0, numRead);
            numRead = src.Read(buffer, 0, buffer.Length);
        }
    }

    /// <summary>
    /// Writes the stream contents to an existing stream.
    /// </summary>
    /// <param name="output">The stream to write to.</param>
    /// <param name="cancellationToken"></param>
    public async virtual Task BuildAsync(Stream output, CancellationToken cancellationToken)
    {
        using var src = await BuildAsync(cancellationToken).ConfigureAwait(false);
        var buffer = new byte[64 * 1024];
        var numRead = await src.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        while (numRead != 0)
        {
            await output.WriteAsync(buffer.AsMemory(0, numRead), cancellationToken).ConfigureAwait(false);
            numRead = await src.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes the stream contents to a file.
    /// </summary>
    /// <param name="outputFile">The file to write to.</param>
    public void Build(string outputFile)
    {
        using var destStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write, FileShare.Delete, bufferSize: 2 << 20, useAsync: false);
        Build(destStream);
    }

    /// <summary>
    /// Writes the stream contents to a file.
    /// </summary>
    /// <param name="outputFile">The file to write to.</param>
    /// <param name="cancellationToken"></param>
    public async Task BuildAsync(string outputFile, CancellationToken cancellationToken)
    {
        using var destStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write, FileShare.Delete, bufferSize: 2 << 20, useAsync: true);
        await BuildAsync(destStream, cancellationToken).ConfigureAwait(false);
    }

    protected abstract List<BuilderExtent> FixExtents(out long totalLength);
}