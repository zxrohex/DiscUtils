using System.IO;
using DiscUtils;
using DiscUtils.Iso9660;
using LibraryTests.Utilities;
using Xunit;

namespace LibraryTests.Iso9660
{
    public class SampleDataTests
    {
        [Fact]
        public void AppleTestZip()
        {
            using var fs = File.OpenRead(Path.Combine("..", "..", "..", "Iso9660", "Data", "apple-test.zip"));
            using var iso = ZipUtilities.ReadFileFromZip(fs);
            using var cr = new CDReader(iso, false);

            var dir = cr.GetDirectoryInfo("sub-directory");
            Assert.NotNull(dir);
            Assert.Equal("sub-directory", dir.Name);

            var file = dir.GetFiles("apple-test.txt");
            Assert.Single(file);
            Assert.Equal(21, file[0].Length);
            Assert.Equal("apple-test.txt", file[0].Name);
            Assert.Equal(dir, file[0].Directory);
        }
    }
}