using DiscUtils.Streams;
using System.Collections.Generic;

namespace DiscUtils;

public interface IAllocationExtentsEnumerable
{
    IEnumerable<StreamExtent> EnumerateAllocationExtents(string path);
}
