using DiscUtils.Streams;
using System.Collections.Generic;

namespace DiscUtils;

public interface IAllocationExtentsFileSystem : IFileSystem
{
    /// <summary>
    /// Converts a file name to the extents containing its data.
    /// </summary>
    /// <param name="path">The path to inspect.</param>
    /// <returns>The file extents, as absolute byte positions in the underlying stream.</returns>
    /// <remarks>Use this method with caution - not all file systems will store all bytes
    /// directly in extents.  Files may be compressed, sparse or encrypted.  This method
    /// merely indicates where file data is stored, not what's stored.</remarks>
    IEnumerable<StreamExtent> PathToExtents(string path);
}
