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
using System.IO;
using DiscUtils.Internal;

namespace DiscUtils.Vmdk;

[VirtualDiskFactory("VMDK", ".vmdk")]
internal sealed class DiskFactory : VirtualDiskFactory
{
    public override string[] Variants
    {
        get { return new[] { "fixed", "dynamic", "vmfsfixed", "vmfsdynamic" }; }
    }

    public override VirtualDiskTypeInfo GetDiskTypeInformation(string variant)
    {
        return MakeDiskTypeInfo(VariantToCreateType(variant));
    }

    public override DiskImageBuilder GetImageBuilder(string variant)
    {
        var builder = new DiskBuilder
        {
            DiskType = VariantToCreateType(variant)
        };
        return builder;
    }

    public override VirtualDisk CreateDisk(FileLocator locator, string variant, string path,
                                           VirtualDiskParameters diskParameters)
    {
        var vmdkParams = new DiskParameters(diskParameters)
        {
            CreateType = VariantToCreateType(variant)
        };
        return Disk.Initialize(locator, path, vmdkParams);
    }

    public override VirtualDisk OpenDisk(string path, FileAccess access, bool useAsync = false)
    {
        return new Disk(path, access, useAsync);
    }

    public override VirtualDisk OpenDisk(FileLocator locator, string path, FileAccess access)
    {
        return new Disk(locator, path, access);
    }

    public override VirtualDiskLayer OpenDiskLayer(FileLocator locator, string path, FileAccess access)
    {
        return new DiskImageFile(locator, path, access);
    }

    internal static VirtualDiskTypeInfo MakeDiskTypeInfo(DiskCreateType createType)
    {
        return new VirtualDiskTypeInfo
        {
            Name = "VMDK",
            Variant = CreateTypeToVariant(createType),
            CanBeHardDisk = true,
            DeterministicGeometry = false,
            PreservesBiosGeometry = false,
            CalcGeometry = c => DiskImageFile.DefaultGeometry(c)
        };
    }

    private static DiskCreateType VariantToCreateType(string variant)
    {
        return variant switch
        {
            "fixed" => DiskCreateType.MonolithicFlat,
            "dynamic" => DiskCreateType.MonolithicSparse,
            "vmfsfixed" => DiskCreateType.Vmfs,
            "vmfsdynamic" => DiskCreateType.VmfsSparse,
            _ => throw new ArgumentException(
                                $"Unknown VMDK disk variant '{variant}'",
                                nameof(variant)),
        };
    }

    private static string CreateTypeToVariant(DiskCreateType createType)
    {
        return createType switch
        {
            DiskCreateType.MonolithicFlat or DiskCreateType.TwoGbMaxExtentFlat => "fixed",
            DiskCreateType.MonolithicSparse or DiskCreateType.TwoGbMaxExtentSparse => "dynamic",
            DiskCreateType.Vmfs => "vmfsfixed",
            DiskCreateType.VmfsSparse => "vmfsdynamic",
            _ => "fixed",
        };
    }
}