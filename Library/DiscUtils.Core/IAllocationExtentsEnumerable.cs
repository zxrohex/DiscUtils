using DiscUtils.Streams;
using System;
using System.Collections.Generic;
using System.Text;

namespace DiscUtils;

public interface IAllocationExtentsEnumerable
{
    IEnumerable<StreamExtent> EnumerateAllocationExtents(string path);
}
