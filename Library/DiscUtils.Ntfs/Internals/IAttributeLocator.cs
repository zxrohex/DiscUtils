namespace DiscUtils.Ntfs.Internals
{
    public interface IAttributeLocator
    {
        ushort Identifier { get; }

        AttributeType AttributeType { get; }

        string Name { get; }

        long FirstFileCluster { get; }
    }
}