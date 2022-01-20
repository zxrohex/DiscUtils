using System;
using System.Collections.Generic;
using System.Text;

namespace DiscUtils.Registry;

internal struct LogEntry
{
    public int SequenceNumber { get; }
    public int PageOffset { get; }
    public int PageSize { get; }
    public int BufferOffset { get; }

    public LogEntry(int sequenceNumber, int pageOffset, int pageSize, int bufferOffset)
    {
        SequenceNumber = sequenceNumber;
        PageOffset = pageOffset;
        PageSize = pageSize;
        BufferOffset = bufferOffset;
    }
}
