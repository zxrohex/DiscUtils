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
using System.Linq;
using DiscUtils;
using DiscUtils.Iso9660;
using Xunit;

namespace LibraryTests.Iso9660
{
    public class IsoDirectoryInfoTest
    {
        [Fact]
        public void Exists()
        {
            CDBuilder builder = new CDBuilder();
            builder.AddFile(@"SOMEDIR\CHILDDIR\FILE.TXT", new byte[0]);
            CDReader fs = new CDReader(builder.Build(), false);

            Assert.True(fs.GetDirectoryInfo(@"\").Exists);
            Assert.True(fs.GetDirectoryInfo(@"SOMEDIR").Exists);
            Assert.True(fs.GetDirectoryInfo(@"SOMEDIR\CHILDDIR").Exists);
            Assert.True(fs.GetDirectoryInfo(@"SOMEDIR\CHILDDIR\").Exists);
            Assert.False(fs.GetDirectoryInfo(@"NONDIR").Exists);
            Assert.False(fs.GetDirectoryInfo(@"SOMEDIR\NONDIR").Exists);
        }

        [Fact]
        public void FullName()
        {
            CDBuilder builder = new CDBuilder();
            CDReader fs = new CDReader(builder.Build(), false);

            var sep = Path.DirectorySeparatorChar;

            Assert.Equal($"{sep}", fs.Root.FullName);
            Assert.Equal($"SOMEDIR{sep}", fs.GetDirectoryInfo($"SOMEDIR").FullName);
            Assert.Equal($"SOMEDIR{sep}CHILDDIR{sep}", fs.GetDirectoryInfo($"SOMEDIR{sep}CHILDDIR").FullName);
        }

        [Fact]
        public void SimpleSearch()
        {
            var sep = Path.DirectorySeparatorChar;

            CDBuilder builder = new CDBuilder();
            builder.AddFile($"SOMEDIR{sep}CHILDDIR{sep}GCHILDIR{sep}FILE.TXT", new byte[0]);
            CDReader fs = new CDReader(builder.Build(), false);

            DiscDirectoryInfo di = fs.GetDirectoryInfo($"SOMEDIR{sep}CHILDDIR");
            DiscFileInfo[] fis = di.GetFiles("*.*", SearchOption.AllDirectories).ToArray();
        }

        [Fact]
        public void Extension()
        {
            CDBuilder builder = new CDBuilder();
            CDReader fs = new CDReader(builder.Build(), false);

            Assert.Equal("dir", fs.GetDirectoryInfo("fred.dir").Extension);
            Assert.Equal("", fs.GetDirectoryInfo("fred").Extension);
        }

        [Fact]
        public void GetDirectories()
        {
            CDBuilder builder = new CDBuilder();
            builder.AddDirectory(@"SOMEDIR\CHILD\GCHILD");
            builder.AddDirectory(@"A.DIR");
            CDReader fs = new CDReader(builder.Build(), false);


            Assert.Equal(2, fs.Root.GetDirectories().Count());

            DiscDirectoryInfo someDir = fs.Root.GetDirectories(@"SoMeDir").First();
            Assert.Single(fs.Root.GetDirectories("SOMEDIR"));
            Assert.Equal("SOMEDIR", someDir.Name);

            Assert.Single(someDir.GetDirectories("*.*"));
            Assert.Equal("CHILD", someDir.GetDirectories("*.*").First().Name);
            Assert.Equal(2, someDir.GetDirectories("*.*", SearchOption.AllDirectories).Count());

            Assert.Equal(4, fs.Root.GetDirectories("*.*", SearchOption.AllDirectories).Count());
            Assert.Equal(2, fs.Root.GetDirectories("*.*", SearchOption.TopDirectoryOnly).Count());

            var sep = Path.DirectorySeparatorChar;

            Assert.Single(fs.Root.GetDirectories("*.DIR", SearchOption.AllDirectories));
            Assert.Equal($"A.DIR{sep}", fs.Root.GetDirectories("*.DIR", SearchOption.AllDirectories).First().FullName);

            Assert.Single(fs.Root.GetDirectories("GCHILD", SearchOption.AllDirectories));
            Assert.Equal($"SOMEDIR{sep}CHILD{sep}GCHILD{sep}", fs.Root.GetDirectories("GCHILD", SearchOption.AllDirectories).First().FullName);
        }

        [Fact]
        public void GetFiles()
        {
            var sep = Path.DirectorySeparatorChar;

            CDBuilder builder = new CDBuilder();
            builder.AddDirectory($"SOMEDIR{sep}CHILD{sep}GCHILD");
            builder.AddDirectory($"AAA.DIR");
            builder.AddFile($"FOO.TXT", new byte[10]);
            builder.AddFile($"SOMEDIR{sep}CHILD.TXT", new byte[10]);
            builder.AddFile($"SOMEDIR{sep}FOO.TXT", new byte[10]);
            builder.AddFile($"SOMEDIR{sep}CHILD{sep}GCHILD{sep}BAR.TXT", new byte[10]);
            CDReader fs = new CDReader(builder.Build(), false);

            Assert.Single(fs.Root.GetFiles());
            Assert.Equal("FOO.TXT", fs.Root.GetFiles().First().FullName);

            Assert.Equal(2, fs.Root.GetDirectories("SOMEDIR").First().GetFiles("*.TXT").Count());
            Assert.Equal(4, fs.Root.GetFiles("*.TXT", SearchOption.AllDirectories).Count());

            Assert.Empty(fs.Root.GetFiles("*.DIR", SearchOption.AllDirectories));
        }

        [Fact]
        public void GetFileSystemInfos()
        {
            var sep = Path.DirectorySeparatorChar;

            CDBuilder builder = new CDBuilder();
            builder.AddDirectory($"SOMEDIR{sep}CHILD{sep}GCHILD");
            builder.AddDirectory($"AAA.EXT");
            builder.AddFile($"FOO.TXT", new byte[10]);
            builder.AddFile($"SOMEDIR{sep}CHILD.TXT", new byte[10]);
            builder.AddFile($"SOMEDIR{sep}FOO.TXT", new byte[10]);
            builder.AddFile($"SOMEDIR{sep}CHILD{sep}GCHILD{sep}BAR.TXT", new byte[10]);
            CDReader fs = new CDReader(builder.Build(), false);

            Assert.Equal(3, fs.Root.GetFileSystemInfos().Count());

            Assert.Single(fs.Root.GetFileSystemInfos("*.EXT"));
            Assert.Equal(2, fs.Root.GetFileSystemInfos("*.?XT").Count());
        }

        [Fact]
        public void Parent()
        {
            CDBuilder builder = new CDBuilder();
            builder.AddDirectory(@"SOMEDIR");
            CDReader fs = new CDReader(builder.Build(), false);

            Assert.Equal(fs.Root, fs.Root.GetDirectories("SOMEDIR").First().Parent);
        }

        [Fact]
        public void Parent_Root()
        {
            CDBuilder builder = new CDBuilder();
            CDReader fs = new CDReader(builder.Build(), false);

            Assert.Null(fs.Root.Parent);
        }

        [Fact]
        public void RootBehaviour()
        {
            // Start time rounded down to whole seconds
            DateTime start = DateTime.UtcNow;
            start = new DateTime(start.Year, start.Month, start.Day, start.Hour, start.Minute, start.Second);

            CDBuilder builder = new CDBuilder();
            CDReader fs = new CDReader(builder.Build(), false);
            DateTime end = DateTime.UtcNow;

            Assert.Equal(FileAttributes.Directory | FileAttributes.ReadOnly, fs.Root.Attributes);
            Assert.True(fs.Root.CreationTimeUtc >= start);
            Assert.True(fs.Root.CreationTimeUtc <= end);
            Assert.True(fs.Root.LastAccessTimeUtc >= start);
            Assert.True(fs.Root.LastAccessTimeUtc <= end);
            Assert.True(fs.Root.LastWriteTimeUtc >= start);
            Assert.True(fs.Root.LastWriteTimeUtc <= end);
        }

        [Fact]
        public void Attributes()
        {
            // Start time rounded down to whole seconds
            DateTime start = DateTime.UtcNow;
            start = new DateTime(start.Year, start.Month, start.Day, start.Hour, start.Minute, start.Second);

            CDBuilder builder = new CDBuilder();
            builder.AddDirectory("Foo");
            CDReader fs = new CDReader(builder.Build(), false);
            DateTime end = DateTime.UtcNow;

            DiscDirectoryInfo di = fs.GetDirectoryInfo("Foo");

            Assert.Equal(FileAttributes.Directory | FileAttributes.ReadOnly, di.Attributes);
            Assert.True(di.CreationTimeUtc >= start);
            Assert.True(di.CreationTimeUtc <= end);
            Assert.True(di.LastAccessTimeUtc >= start);
            Assert.True(di.LastAccessTimeUtc <= end);
            Assert.True(di.LastWriteTimeUtc >= start);
            Assert.True(di.LastWriteTimeUtc <= end);
        }
    }
}
