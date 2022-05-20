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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using DiscUtils.Streams;

namespace DiscUtils.Vmdk;

internal class DescriptorFile
{
    private const string HeaderVersion = "version";
    private const string HeaderContentId = "CID";
    private const string HeaderParentContentId = "parentCID";
    private const string HeaderCreateType = "createType";
    private const string HeaderParentFileNameHint = "parentFileNameHint";

    private const string DiskDbAdapterType = "ddb.adapterType";
    private const string DiskDbSectors = "ddb.geometry.sectors";
    private const string DiskDbHeads = "ddb.geometry.heads";
    private const string DiskDbCylinders = "ddb.geometry.cylinders";
    private const string DiskDbBiosSectors = "ddb.geometry.biosSectors";
    private const string DiskDbBiosHeads = "ddb.geometry.biosHeads";
    private const string DiskDbBiosCylinders = "ddb.geometry.biosCylinders";
    private const string DiskDbHardwareVersion = "ddb.virtualHWVersion";
    private const string DiskDbUuid = "ddb.uuid";

    private const long MaxSize = 20 * Sizes.OneKiB;
    private readonly List<DescriptorFileEntry> _diskDataBase;

    private readonly List<DescriptorFileEntry> _header;

    public DescriptorFile()
    {
        _header = new List<DescriptorFileEntry>();
        Extents = new List<ExtentDescriptor>();
        _diskDataBase = new List<DescriptorFileEntry>();

        _header.Add(new DescriptorFileEntry(HeaderVersion, "1", DescriptorFileEntryType.Plain));
        _header.Add(new DescriptorFileEntry(HeaderContentId, "ffffffff", DescriptorFileEntryType.Plain));
        _header.Add(new DescriptorFileEntry(HeaderParentContentId, "ffffffff", DescriptorFileEntryType.Plain));
        _header.Add(new DescriptorFileEntry(HeaderCreateType, string.Empty, DescriptorFileEntryType.Quoted));
    }

    public DescriptorFile(Stream source)
    {
        _header = new List<DescriptorFileEntry>();
        Extents = new List<ExtentDescriptor>();
        _diskDataBase = new List<DescriptorFileEntry>();

        Load(source);
    }

    public DiskAdapterType AdapterType
    {
        get { return ParseAdapterType(GetDiskDatabase(DiskDbAdapterType)); }
        set { SetDiskDatabase(DiskDbAdapterType, FormatAdapterType(value)); }
    }

    public Geometry BiosGeometry
    {
        get
        {
            var cylStr = GetDiskDatabase(DiskDbBiosCylinders);
            var headsStr = GetDiskDatabase(DiskDbBiosHeads);
            var sectorsStr = GetDiskDatabase(DiskDbBiosSectors);
            if (!string.IsNullOrEmpty(cylStr) && !string.IsNullOrEmpty(headsStr) &&
                !string.IsNullOrEmpty(sectorsStr))
            {
                return new Geometry(
                    int.Parse(cylStr, CultureInfo.InvariantCulture),
                    int.Parse(headsStr, CultureInfo.InvariantCulture),
                    int.Parse(sectorsStr, CultureInfo.InvariantCulture));
            }

            return null;
        }

        set
        {
            SetDiskDatabase(DiskDbBiosCylinders, value.Cylinders.ToString(CultureInfo.InvariantCulture));
            SetDiskDatabase(DiskDbBiosHeads, value.HeadsPerCylinder.ToString(CultureInfo.InvariantCulture));
            SetDiskDatabase(DiskDbBiosSectors, value.SectorsPerTrack.ToString(CultureInfo.InvariantCulture));
        }
    }

    public uint ContentId
    {
        get { return uint.Parse(GetHeader(HeaderContentId), NumberStyles.HexNumber, CultureInfo.InvariantCulture); }
        set
        {
            SetHeader(HeaderContentId, value.ToString("x8", CultureInfo.InvariantCulture),
                DescriptorFileEntryType.Plain);
        }
    }

    public DiskCreateType CreateType
    {
        get { return ParseCreateType(GetHeader(HeaderCreateType)); }
        set { SetHeader(HeaderCreateType, FormatCreateType(value), DescriptorFileEntryType.Plain); }
    }

    public Geometry DiskGeometry
    {
        get
        {
            var cylStr = GetDiskDatabase(DiskDbCylinders);
            var headsStr = GetDiskDatabase(DiskDbHeads);
            var sectorsStr = GetDiskDatabase(DiskDbSectors);
            if (!string.IsNullOrEmpty(cylStr) && !string.IsNullOrEmpty(headsStr) &&
                !string.IsNullOrEmpty(sectorsStr))
            {
                return new Geometry(
                    int.Parse(cylStr, CultureInfo.InvariantCulture),
                    int.Parse(headsStr, CultureInfo.InvariantCulture),
                    int.Parse(sectorsStr, CultureInfo.InvariantCulture));
            }

            return null;
        }

        set
        {
            SetDiskDatabase(DiskDbCylinders, value.Cylinders.ToString(CultureInfo.InvariantCulture));
            SetDiskDatabase(DiskDbHeads, value.HeadsPerCylinder.ToString(CultureInfo.InvariantCulture));
            SetDiskDatabase(DiskDbSectors, value.SectorsPerTrack.ToString(CultureInfo.InvariantCulture));
        }
    }

    public List<ExtentDescriptor> Extents { get; }

    public string HardwareVersion
    {
        get { return GetDiskDatabase(DiskDbHardwareVersion); }
        set { SetDiskDatabase(DiskDbHardwareVersion, value); }
    }

    public uint ParentContentId
    {
        get { return uint.Parse(GetHeader(HeaderParentContentId), NumberStyles.HexNumber, CultureInfo.InvariantCulture); }
        set
        {
            SetHeader(HeaderParentContentId, value.ToString("x8", CultureInfo.InvariantCulture),
                DescriptorFileEntryType.Plain);
        }
    }

    public string ParentFileNameHint
    {
        get { return GetHeader(HeaderParentFileNameHint); }
        set { SetHeader(HeaderParentFileNameHint, value, DescriptorFileEntryType.Quoted); }
    }

    public Guid UniqueId
    {
        get { return ParseUuid(GetDiskDatabase(DiskDbUuid)); }
        set { SetDiskDatabase(DiskDbUuid, FormatUuid(value)); }
    }

    internal void Write(Stream stream)
    {
        var content = new StringBuilder();

        content.Append("# Disk DescriptorFile\n");
        for (var i = 0; i < _header.Count; ++i)
        {
            content.Append(_header[i].ToString(false) + "\n");
        }

        content.Append("\n");
        content.Append("# Extent description\n");
        for (var i = 0; i < Extents.Count; ++i)
        {
            content.Append(Extents[i] + "\n");
        }

        content.Append("\n");
        content.Append("# The Disk Data Base\n");
        content.Append("#DDB\n");
        for (var i = 0; i < _diskDataBase.Count; ++i)
        {
            content.Append(_diskDataBase[i].ToString(true) + "\n");
        }

        var contentBytes = Encoding.ASCII.GetBytes(content.ToString());
        stream.Write(contentBytes, 0, contentBytes.Length);
    }

    private static DiskAdapterType ParseAdapterType(string value)
    {
        return value switch
        {
            "ide" => DiskAdapterType.Ide,
            "buslogic" => DiskAdapterType.BusLogicScsi,
            "lsilogic" => DiskAdapterType.LsiLogicScsi,
            "legacyESX" => DiskAdapterType.LegacyEsx,
            _ => throw new ArgumentException(
                                $"Unknown type: {value}", nameof(value)),
        };
    }

    private static string FormatAdapterType(DiskAdapterType value)
    {
        return value switch
        {
            DiskAdapterType.Ide => "ide",
            DiskAdapterType.BusLogicScsi => "buslogic",
            DiskAdapterType.LsiLogicScsi => "lsilogic",
            DiskAdapterType.LegacyEsx => "legacyESX",
            _ => throw new ArgumentException(
                                $"Unknown type: {value}", nameof(value)),
        };
    }

    private static DiskCreateType ParseCreateType(string value)
    {
        return value switch
        {
            "monolithicSparse" => DiskCreateType.MonolithicSparse,
            "vmfsSparse" => DiskCreateType.VmfsSparse,
            "monolithicFlat" => DiskCreateType.MonolithicFlat,
            "vmfs" => DiskCreateType.Vmfs,
            "twoGbMaxExtentSparse" => DiskCreateType.TwoGbMaxExtentSparse,
            "twoGbMaxExtentFlat" => DiskCreateType.TwoGbMaxExtentFlat,
            "fullDevice" => DiskCreateType.FullDevice,
            "vmfsRaw" => DiskCreateType.VmfsRaw,
            "partitionedDevice" => DiskCreateType.PartitionedDevice,
            "vmfsRawDeviceMap" => DiskCreateType.VmfsRawDeviceMap,
            "vmfsPassthroughRawDeviceMap" => DiskCreateType.VmfsPassthroughRawDeviceMap,
            "streamOptimized" => DiskCreateType.StreamOptimized,
            "seSparse" => DiskCreateType.SeSparse,
            "vsanSparse" => DiskCreateType.VsanSparse,
            _ => throw new ArgumentException(
                                $"Unknown type: {value}", nameof(value)),
        };
    }

    private static string FormatCreateType(DiskCreateType value)
    {
        return value switch
        {
            DiskCreateType.MonolithicSparse => "monolithicSparse",
            DiskCreateType.VmfsSparse => "vmfsSparse",
            DiskCreateType.MonolithicFlat => "monolithicFlat",
            DiskCreateType.Vmfs => "vmfs",
            DiskCreateType.TwoGbMaxExtentSparse => "twoGbMaxExtentSparse",
            DiskCreateType.TwoGbMaxExtentFlat => "twoGbMaxExtentFlat",
            DiskCreateType.FullDevice => "fullDevice",
            DiskCreateType.VmfsRaw => "vmfsRaw",
            DiskCreateType.PartitionedDevice => "partitionedDevice",
            DiskCreateType.VmfsRawDeviceMap => "vmfsRawDeviceMap",
            DiskCreateType.VmfsPassthroughRawDeviceMap => "vmfsPassthroughRawDeviceMap",
            DiskCreateType.StreamOptimized => "streamOptimized",
            DiskCreateType.SeSparse => "seSparse",
            DiskCreateType.VsanSparse => "vsanSparse",
            _ => throw new ArgumentException(
                                $"Unknown type: {value}", nameof(value)),
        };
    }

    private static Guid ParseUuid(string value)
    {
        Span<byte> data = stackalloc byte[16];
        var bytesAsHex = value.Split(' ', '-');
        if (bytesAsHex.Length != 16)
        {
            throw new ArgumentException("Invalid UUID", nameof(value));
        }

        for (var i = 0; i < 16; ++i)
        {
            data[i] = byte.Parse(bytesAsHex[i], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return MemoryMarshal.Read<Guid>(data);
    }

    private static string FormatUuid(Guid value)
    {
        var data = value.ToByteArray();
        return string.Format(
            CultureInfo.InvariantCulture,
            "{0:x2} {1:x2} {2:x2} {3:x2} {4:x2} {5:x2} {6:x2} {7:x2}-{8:x2} {9:x2} {10:x2} {11:x2} {12:x2} {13:x2} {14:x2} {15:x2}",
            data[0],
            data[1],
            data[2],
            data[3],
            data[4],
            data[5],
            data[6],
            data[7],
            data[8],
            data[9],
            data[10],
            data[11],
            data[12],
            data[13],
            data[14],
            data[15]);
    }

    private string GetHeader(string key)
    {
        foreach (var entry in _header)
        {
            if (entry.Key == key)
            {
                return entry.Value;
            }
        }

        return null;
    }

    private void SetHeader(string key, string newValue, DescriptorFileEntryType type)
    {
        foreach (var entry in _header)
        {
            if (entry.Key == key)
            {
                entry.Value = newValue;
                return;
            }
        }

        _header.Add(new DescriptorFileEntry(key, newValue, type));
    }

    private string GetDiskDatabase(string key)
    {
        foreach (var entry in _diskDataBase)
        {
            if (entry.Key == key)
            {
                return entry.Value;
            }
        }

        return null;
    }

    private void SetDiskDatabase(string key, string value)
    {
        foreach (var entry in _diskDataBase)
        {
            if (entry.Key == key)
            {
                entry.Value = value;
                return;
            }
        }

        _diskDataBase.Add(new DescriptorFileEntry(key, value, DescriptorFileEntryType.Quoted));
    }

    private void Load(Stream source)
    {
        var descriptor_size = source.Length - source.Position;

        var reader = new StreamReader(source);
        
        var line = reader.ReadLine();
        
        if (line != null &&
            !line.Equals("# Disk DescriptorFile", StringComparison.OrdinalIgnoreCase) &&
            descriptor_size > MaxSize)
        {
            throw new IOException($"Too large VMDK descriptor file, {descriptor_size} bytes. Largest allowed size is {MaxSize} bytes. Please verify that you open a descriptor VMDK file and not an actual image file.");
        }

        while (line != null)
        {
            line = line.Trim('\0');

            var commentPos = line.IndexOf('#');
            if (commentPos >= 0)
            {
                line = line.Substring(0, commentPos);
            }

            if (line.Length > 0)
            {
                if (line.StartsWith("RW", StringComparison.Ordinal)
                    || line.StartsWith("RDONLY", StringComparison.Ordinal)
                    || line.StartsWith("NOACCESS", StringComparison.Ordinal))
                {
                    Extents.Add(ExtentDescriptor.Parse(line));
                }
                else
                {
                    var entry = DescriptorFileEntry.Parse(line);
                    if (entry.Key.StartsWith("ddb.", StringComparison.Ordinal))
                    {
                        _diskDataBase.Add(entry);
                    }
                    else
                    {
                        _header.Add(entry);
                    }
                }
            }

            line = reader.ReadLine();
        }
    }
}