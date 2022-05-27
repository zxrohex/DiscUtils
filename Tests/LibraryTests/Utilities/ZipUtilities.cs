using DiscUtils.Streams;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace LibraryTests.Utilities
{
    public static class ZipUtilities
    {
        public static Stream ReadFileFromZip(Stream zip, string name = null)
        {
            using var zipArchive = new ZipArchive(zip, ZipArchiveMode.Read, true);
            ZipArchiveEntry entry;
            if (name == null)
                entry = zipArchive.Entries.First();
            else
                entry = zipArchive.GetEntry(name);

            Stream ms;

            if (entry.Length > 100 * Sizes.OneMiB)
            {
                var tempFile = Path.GetTempFileName();
                ms = new FileStream(tempFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.Asynchronous);
            }
            else
            {
                ms = new MemoryStream();
            }

            using (var zipFile = entry.Open())
                zipFile.CopyTo(ms);

            return ms;
        }
    }
}
