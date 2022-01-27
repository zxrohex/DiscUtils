namespace DiscUtils.Registry;

internal record struct LogEntry(int SequenceNumber, int PageOffset, int PageSize, int BufferOffset);
