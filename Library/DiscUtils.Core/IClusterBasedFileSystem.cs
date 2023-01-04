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

using DiscUtils.Streams;
using System.Collections.Generic;

namespace DiscUtils;

/// <summary>
/// Base class for all file systems based on a cluster model.
/// </summary>
public interface IClusterBasedFileSystem : IFileSystem, IAllocationExtentsFileSystem
{
    /// <summary>
    /// Gets the size (in bytes) of each cluster.
    /// </summary>
    long ClusterSize { get; }

    /// <summary>
    /// Gets the total number of clusters managed by the file system.
    /// </summary>
    long TotalClusters { get; }

    /// <summary>
    /// Gets the size (in bytes) of each sector.
    /// </summary>
    int SectorSize { get; }

    /// <summary>
    /// Gets the total number of sectors managed by the file system.
    /// </summary>
    long TotalSectors { get; }

    /// <summary>
    /// Converts a cluster (index) into an absolute byte position in the underlying stream.
    /// </summary>
    /// <param name="cluster">The cluster to convert.</param>
    /// <returns>The corresponding absolute byte position.</returns>
    long ClusterToOffset(long cluster);

    /// <summary>
    /// Converts an absolute byte position in the underlying stream to a cluster (index).
    /// </summary>
    /// <param name="offset">The byte position to convert.</param>
    /// <returns>The cluster containing the specified byte.</returns>
    long OffsetToCluster(long offset);

    /// <summary>
    /// Converts a file name to the list of clusters occupied by the file's data.
    /// </summary>
    /// <param name="path">The path to inspect.</param>
    /// <returns>The clusters.</returns>
    /// <remarks>Note that in some file systems, small files may not have dedicated
    /// clusters.  Only dedicated clusters will be returned.</remarks>
    IEnumerable<Range<long, long>> PathToClusters(string path);

    /// <summary>
    /// Gets number of allocated clusters for a file.
    /// </summary>
    /// <param name="path">The path to inspect.</param>
    /// <returns>Number of clusters allocated</returns>
    long GetAllocatedClustersCount(string path);
}