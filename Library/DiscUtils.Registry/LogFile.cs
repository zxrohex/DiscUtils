using DiscUtils.Streams;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Diagnostics;

namespace DiscUtils.Registry;

internal class LogFile
{
    const int LOG_ENTRY = 1162638920;

    private byte[] buffer;

    public LogFile(Stream stream)
    {
        stream.Position = 0;
        buffer = StreamUtilities.ReadExact(stream, (int)stream.Length);
    }

    public IEnumerable<LogEntry> EnumerateEntries()
    {
        for (var offset = 0x200; offset < buffer.Length - HiveHeader.HeaderSize;)
        {
            var signature = EndianUtilities.ToInt32LittleEndian(buffer, offset);
            if (signature != LOG_ENTRY)
            {
                yield break;
            }

            var size = EndianUtilities.ToInt32LittleEndian(buffer, offset + 4);
            var sequence_number = EndianUtilities.ToInt32LittleEndian(buffer, offset + 12);
            var dirty_page_count = EndianUtilities.ToInt32LittleEndian(buffer, offset + 20);

            for (int i = 0, dataoffset = offset + 40 + dirty_page_count * 8;
                i < dirty_page_count;
                i++)
            {
                var pageoffset = EndianUtilities.ToInt32LittleEndian(buffer, offset + 40 + i * 8);
                var pagesize = EndianUtilities.ToInt32LittleEndian(buffer, offset + 40 + i * 8 + 4);
                yield return new(sequence_number, pageoffset, pagesize, dataoffset);
                dataoffset += pagesize;
            }

            offset += size;
        }
    }

    public int UpdateHive(Stream hive)
    {
        int sequenceNumber = 0;

        foreach (var entry in EnumerateEntries())
        {
#if DEBUG
            Trace.WriteLine($"Replaying log entry {entry}");
#endif
            hive.Position = 0x1000 + entry.PageOffset;
            hive.Write(buffer, entry.BufferOffset, entry.PageSize);
            sequenceNumber = entry.SequenceNumber;
        }

        return sequenceNumber;
    }
}

