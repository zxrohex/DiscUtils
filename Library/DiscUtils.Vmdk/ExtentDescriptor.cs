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
using System.Linq;

namespace DiscUtils.Vmdk;

internal class ExtentDescriptor
{
    public ExtentDescriptor() {}

    public ExtentDescriptor(ExtentAccess access, long size, ExtentType type, string fileName, long offset)
    {
        Access = access;
        SizeInSectors = size;
        Type = type;
        FileName = fileName;
        Offset = offset;
    }

    public ExtentAccess Access { get; private set; }

    public string FileName { get; private set; }

    public long Offset { get; private set; }

    public long SizeInSectors { get; private set; }

    public ExtentType Type { get; private set; }

    public static ExtentDescriptor Parse(string descriptor)
    {
        var elems = SplitQuotedString(descriptor).ToArray();
        if (elems.Length < 4)
        {
            throw new IOException($"Invalid extent descriptor: {descriptor}");
        }

        var result = new ExtentDescriptor
        {
            Access = ParseAccess(elems[0]),
            SizeInSectors = long.Parse(elems[1], CultureInfo.InvariantCulture),
            Type = ParseType(elems[2]),
            FileName = elems[3].Trim('\"')
        };
        if (elems.Length > 4)
        {
            result.Offset = long.Parse(elems[4], CultureInfo.InvariantCulture);
        }

        return result;
    }

    public static ExtentAccess ParseAccess(string access)
    {
        if (access == "NOACCESS")
        {
            return ExtentAccess.None;
        }
        if (access == "RDONLY")
        {
            return ExtentAccess.ReadOnly;
        }
        if (access == "RW")
        {
            return ExtentAccess.ReadWrite;
        }
        throw new ArgumentException("Unknown access type", nameof(access));
    }

    public static string FormatAccess(ExtentAccess access)
    {
        return access switch
        {
            ExtentAccess.None => "NOACCESS",
            ExtentAccess.ReadOnly => "RDONLY",
            ExtentAccess.ReadWrite => "RW",
            _ => throw new ArgumentException("Unknown access type", nameof(access)),
        };
    }

    public static ExtentType ParseType(string type)
    {
        if (type == "FLAT")
        {
            return ExtentType.Flat;
        }
        if (type == "SPARSE")
        {
            return ExtentType.Sparse;
        }
        if (type == "ZERO")
        {
            return ExtentType.Zero;
        }
        if (type == "VMFS")
        {
            return ExtentType.Vmfs;
        }
        if (type == "VMFSSPARSE")
        {
            return ExtentType.VmfsSparse;
        }
        if (type == "VMFSRDM")
        {
            return ExtentType.VmfsRdm;
        }
        if (type == "VMFSRAW")
        {
            return ExtentType.VmfsRaw;
        }
        if (type == "SESPARSE")
        {
            return ExtentType.SeSparse;
        }
        if (type == "VSANSPARSE")
        {
            return ExtentType.VsanSparse;
        }
        throw new ArgumentException("Unknown extent type", nameof(type));
    }

    public static string FormatExtentType(ExtentType type)
    {
        return type switch
        {
            ExtentType.Flat => "FLAT",
            ExtentType.Sparse => "SPARSE",
            ExtentType.Zero => "ZERO",
            ExtentType.Vmfs => "VMFS",
            ExtentType.VmfsSparse => "VMFSSPARSE",
            ExtentType.VmfsRdm => "VMFSRDM",
            ExtentType.VmfsRaw => "VMFSRAW",
            ExtentType.SeSparse => "SESPARSE",
            ExtentType.VsanSparse => "VSANSPARSE",
            _ => throw new ArgumentException("Unknown extent type", nameof(type)),
        };
    }

    public override string ToString()
    {
        var basic = FormatAccess(Access) + " " + SizeInSectors + " " + FormatExtentType(Type) + " \"" +
                       FileName + "\"";
        if (Type != ExtentType.Sparse && Type != ExtentType.VmfsSparse && Type != ExtentType.Zero)
        {
            return basic + " " + Offset;
        }

        return basic;
    }

    private static IEnumerable<string> SplitQuotedString(string source)
    {
        var idx = 0;
        while (idx < source.Length)
        {
            // Skip spaces
            while (source[idx] == ' ' && idx < source.Length)
            {
                idx++;
            }

            if (source[idx] == '"')
            {
                // A quoted value, find end of quotes...
                var start = idx;
                idx++;
                while (idx < source.Length && source[idx] != '"')
                {
                    idx++;
                }

                yield return source.Substring(start, idx - start + 1);
            }
            else
            {
                // An unquoted value, find end of value
                var start = idx;
                idx++;
                while (idx < source.Length && source[idx] != ' ')
                {
                    idx++;
                }

                yield return source.Substring(start, idx - start);
            }

            idx++;
        }
    }
}