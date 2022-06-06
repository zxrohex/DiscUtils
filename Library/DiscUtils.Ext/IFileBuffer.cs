using DiscUtils.Streams;
using System.Collections.Generic;

namespace DiscUtils.Ext;

internal interface IFileBuffer : IBuffer
{
    IEnumerable<StreamExtent> EnumerateAllocationExtents();
}
