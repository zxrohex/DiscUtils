using System;

namespace DiscUtils.Core.WindowsSecurity.AccessControl;

public class RawSecurityDescriptor : GenericSecurityDescriptor
{
    private ControlFlags _controlFlags;

    internal override GenericAcl InternalDacl => DiscretionaryAcl;
    internal override GenericAcl InternalSacl => SystemAcl;
    internal override byte InternalReservedField => ResourceManagerControl;
    
    public override ControlFlags ControlFlags => _controlFlags;

    public RawAcl DiscretionaryAcl { get; set; }
    public override SecurityIdentifier Group { get; set; }
    public override SecurityIdentifier Owner { get; set; }
    public byte ResourceManagerControl { get; set; }
    public RawAcl SystemAcl { get; set; }
    
    public RawSecurityDescriptor(string sddlForm)
    {
        if (sddlForm == null)
        {
            throw new ArgumentNullException(nameof(sddlForm));
        }

        SetSddlForm(sddlForm.Replace(" ", ""));

        _controlFlags |= ControlFlags.SelfRelative;
    }

    public RawSecurityDescriptor(byte[] binaryForm, int offset)
    {
        if (binaryForm == null)
        {
            throw new ArgumentNullException(nameof(binaryForm));
        }

        if (offset < 0 || offset > binaryForm.Length - 0x14)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset out of range");
        }

        if (binaryForm[offset] != 1)
        {
            throw new ArgumentException("Unrecognized Security Descriptor revision.", nameof(binaryForm));
        }

        ResourceManagerControl = binaryForm[offset + 0x01];
        _controlFlags = (ControlFlags)ReadUShort(binaryForm, offset + 0x02);

        var ownerPos = ReadInt(binaryForm, offset + 0x04);
        var groupPos = ReadInt(binaryForm, offset + 0x08);
        var saclPos = ReadInt(binaryForm, offset + 0x0C);
        var daclPos = ReadInt(binaryForm, offset + 0x10);

        if (ownerPos != 0)
        {
            Owner = new SecurityIdentifier(binaryForm, ownerPos);
        }

        if (groupPos != 0)
        {
            Group = new SecurityIdentifier(binaryForm, groupPos);
        }

        if (saclPos != 0)
        {
            SystemAcl = new RawAcl(binaryForm, saclPos);
        }

        if (daclPos != 0)
        {
            DiscretionaryAcl = new RawAcl(binaryForm, daclPos);
        }
    }

    private RawSecurityDescriptor() { }

    public static bool TryParse(byte[] binaryForm, int offset, out RawSecurityDescriptor securityDescriptor)
    {
        securityDescriptor = null;

        if (binaryForm == null || offset < 0 || offset > binaryForm.Length - 0x14 || binaryForm[offset] != 1)
        {
            return false;
        }

        var sd = new RawSecurityDescriptor
        {
            ResourceManagerControl = binaryForm[offset + 0x01],
            _controlFlags = (ControlFlags)ReadUShort(binaryForm, offset + 0x02)
        };

        var ownerPos = ReadInt(binaryForm, offset + 0x04);
        var groupPos = ReadInt(binaryForm, offset + 0x08);
        var saclPos = ReadInt(binaryForm, offset + 0x0C);
        var daclPos = ReadInt(binaryForm, offset + 0x10);

        if (ownerPos != 0)
        {
            if (!SecurityIdentifier.TryParse(binaryForm, ownerPos, out var owner))
            {
                return false;
            }
            sd.Owner = owner;
        }

        if (groupPos != 0)
        {
            if (!SecurityIdentifier.TryParse(binaryForm, groupPos, out var group))
            {
                return false;
            }
            sd.Group = group;
        }

        if (saclPos != 0)
        {
            if (!RawAcl.TryParse(binaryForm, saclPos, out var systemAcl))
            {
                return false;
            }
            sd.SystemAcl = systemAcl;
        }

        if (daclPos != 0)
        {
            if (!RawAcl.TryParse(binaryForm, daclPos, out var discretionaryAcl))
            {
                return false;
            }
            sd.DiscretionaryAcl = discretionaryAcl;
        }

        securityDescriptor = sd;
        return true;
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public RawSecurityDescriptor(ReadOnlySpan<byte> binaryForm)
    {
        if (binaryForm.IsEmpty || binaryForm.Length < 0x14)
        {
            throw new ArgumentOutOfRangeException(nameof(binaryForm), binaryForm.Length, "Binary length out of range");
        }

        if (binaryForm[0] != 1)
        {
            throw new ArgumentException("Unrecognized Security Descriptor revision.", nameof(binaryForm));
        }

        ResourceManagerControl = binaryForm[0x01];
        _controlFlags = (ControlFlags)ReadUShort(binaryForm[0x02..]);

        var ownerPos = ReadInt(binaryForm[0x04..]);
        var groupPos = ReadInt(binaryForm[0x08..]);
        var saclPos = ReadInt(binaryForm[0x0C..]);
        var daclPos = ReadInt(binaryForm[0x10..]);

        if (ownerPos != 0)
        {
            Owner = new SecurityIdentifier(binaryForm[ownerPos..]);
        }

        if (groupPos != 0)
        {
            Group = new SecurityIdentifier(binaryForm[groupPos..]);
        }

        if (saclPos != 0)
        {
            SystemAcl = new RawAcl(binaryForm[saclPos..]);
        }

        if (daclPos != 0)
        {
            DiscretionaryAcl = new RawAcl(binaryForm[daclPos..]);
        }
    }

    public static bool TryParse(ReadOnlySpan<byte> binaryForm, out RawSecurityDescriptor securityDescriptor)
    {
        securityDescriptor = null;

        if (binaryForm == null || 0x14 > binaryForm.Length || binaryForm[0] != 1)
        {
            return false;
        }

        var sd = new RawSecurityDescriptor
        {
            ResourceManagerControl = binaryForm[0x01],
            _controlFlags = (ControlFlags)ReadUShort(binaryForm[0x02..])
        };

        var ownerPos = ReadInt(binaryForm[0x04..]);
        var groupPos = ReadInt(binaryForm[0x08..]);
        var saclPos = ReadInt(binaryForm[0x0C..]);
        var daclPos = ReadInt(binaryForm[0x10..]);

        if (ownerPos != 0)
        {
            if (!SecurityIdentifier.TryParse(binaryForm[ownerPos..], out var owner))
            {
                return false;
            }
            sd.Owner = owner;
        }

        if (groupPos != 0)
        {
            if (!SecurityIdentifier.TryParse(binaryForm[groupPos..], out var group))
            {
                return false;
            }
            sd.Group = group;
        }

        if (saclPos != 0)
        {
            if (!RawAcl.TryParse(binaryForm[saclPos..], out var systemAcl))
            {
                return false;
            }
            sd.SystemAcl = systemAcl;
        }

        if (daclPos != 0)
        {
            if (!RawAcl.TryParse(binaryForm[daclPos..], out var discretionaryAcl))
            {
                return false;
            }
            sd.DiscretionaryAcl = discretionaryAcl;
        }

        securityDescriptor = sd;
        return true;
    }

#endif

    public RawSecurityDescriptor(ControlFlags flags,
                                 SecurityIdentifier owner,
                                 SecurityIdentifier group,
                                 RawAcl systemAcl,
                                 RawAcl discretionaryAcl)
    {
        _controlFlags = flags;
        Owner = owner;
        Group = group;
        SystemAcl = systemAcl;
        DiscretionaryAcl = discretionaryAcl;
    }

    public void SetSddlForm(string sddlForm)
    {
        var flags = ControlFlags.None;

        var pos = 0;
        while (pos < sddlForm.Length - 2)
        {
            switch (sddlForm.Substring(pos, 2))
            {
                case "O:":
                    pos += 2;
                    Owner = SecurityIdentifier.ParseSddlForm(sddlForm, ref pos);
                    break;

                case "G:":
                    pos += 2;
                    Group = SecurityIdentifier.ParseSddlForm(sddlForm, ref pos);
                    break;

                case "D:":
                    pos += 2;
                    DiscretionaryAcl = RawAcl.ParseSddlForm(sddlForm, true, ref flags, ref pos);
                    flags |= ControlFlags.DiscretionaryAclPresent;
                    break;

                case "S:":
                    pos += 2;
                    SystemAcl = RawAcl.ParseSddlForm(sddlForm, false, ref flags, ref pos);
                    flags |= ControlFlags.SystemAclPresent;
                    break;
                default:

                    throw new ArgumentException("Invalid SDDL.", nameof(sddlForm));
            }
        }

        if (pos != sddlForm.Length)
        {
            throw new ArgumentException("Invalid SDDL.", nameof(sddlForm));
        }

        SetFlags(flags);
    }

    public void SetFlags(ControlFlags flags) => _controlFlags = flags | ControlFlags.SelfRelative;

    private static ushort ReadUShort(byte[] buffer, int offset)
    {
        return (ushort)((((int)buffer[offset + 0]) << 0)
                        | (((int)buffer[offset + 1]) << 8));
    }

    private static int ReadInt(byte[] buffer, int offset)
    {
        return (((int)buffer[offset + 0]) << 0)
               | (((int)buffer[offset + 1]) << 8)
               | (((int)buffer[offset + 2]) << 16)
               | (((int)buffer[offset + 3]) << 24);
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    private static ushort ReadUShort(ReadOnlySpan<byte> buffer)
    {
        return (ushort)((((int)buffer[0]) << 0)
                        | (((int)buffer[1]) << 8));
    }

    private static int ReadInt(ReadOnlySpan<byte> buffer)
    {
        return (((int)buffer[0]) << 0)
               | (((int)buffer[1]) << 8)
               | (((int)buffer[2]) << 16)
               | (((int)buffer[3]) << 24);
    }
#endif
}