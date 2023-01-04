namespace DiscUtils;

public interface IFileSystemWithClusterMap : IClusterBasedFileSystem
{
    /// <summary>
    /// Gets an object that can convert between clusters and files.
    /// </summary>
    /// <returns>The cluster map.</returns>
    ClusterMap BuildClusterMap();
}
