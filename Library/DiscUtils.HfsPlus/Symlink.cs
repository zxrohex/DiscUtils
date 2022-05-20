using System.IO;
using DiscUtils.Streams;
using DiscUtils.Vfs;

namespace DiscUtils.HfsPlus;

internal class Symlink : File, IVfsSymlink<DirEntry, File>
{
    private string _targetPath;

    public Symlink(Context context, CatalogNodeId nodeId, CommonCatalogFileInfo catalogInfo)
        : base(context, nodeId, catalogInfo) {}

    public string TargetPath
    {
        get
        {
            if (_targetPath == null)
            {
                using var stream = new BufferStream(FileContent, FileAccess.Read);
                using var reader = new StreamReader(stream);
                _targetPath = reader.ReadToEnd();
                _targetPath = _targetPath.Replace('/', Path.DirectorySeparatorChar);
            }

            return _targetPath;
        }
    }
}