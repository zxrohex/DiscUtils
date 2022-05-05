using DiscUtils.Streams;
using System;
using System.Collections.Generic;
using System.Text;

namespace DiscUtils.Ext;

internal interface IFileBuffer : IBuffer
{
    IEnumerable<StreamExtent> EnumerateAllocationExtents();
}
