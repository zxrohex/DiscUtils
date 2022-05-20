using System;
using System.Globalization;

namespace DiscUtils.Core.WindowsSecurity.AccessControl;

public sealed class CommonAce : QualifiedAce
{
    public override int BinaryLength => 8 + SecurityIdentifier.BinaryLength
                                          + OpaqueLength;
    
    public CommonAce(AceFlags flags, AceQualifier qualifier,
                     int accessMask, SecurityIdentifier sid,
                     bool isCallback, byte[] opaque)
        : base(ConvertType(qualifier, isCallback),
            flags,
            opaque)
    {
        AccessMask = accessMask;
        SecurityIdentifier = sid;
    }

    internal CommonAce(AceType type, AceFlags flags, int accessMask,
                       SecurityIdentifier sid, byte[] opaque)
        : base(type, flags, opaque)
    {
        AccessMask = accessMask;
        SecurityIdentifier = sid;
    }

    internal CommonAce(byte[] binaryForm, int offset)
        : base(binaryForm, offset)
    {
        int len = ReadUShort(binaryForm, offset + 2);
        if (offset > binaryForm.Length - len)
        {
            throw new ArgumentException("Invalid ACE - truncated", nameof(binaryForm));
        }

        if (len < 8 + SecurityIdentifier.MinBinaryLength)
        {
            throw new ArgumentException("Invalid ACE", nameof(binaryForm));
        }

        AccessMask = ReadInt(binaryForm, offset + 4);
        SecurityIdentifier = new SecurityIdentifier(binaryForm,
            offset + 8);

        var opaqueLen = len - (8 + SecurityIdentifier.BinaryLength);
        if (opaqueLen > 0)
        {
            Span<byte> opaque = stackalloc byte[opaqueLen];
            binaryForm.AsSpan(
                offset + 8 + SecurityIdentifier.BinaryLength,
                opaqueLen).CopyTo(opaque);
            SetOpaque(opaque);
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    internal CommonAce(ReadOnlySpan<byte> binaryForm)
        : base(binaryForm)
    {
        int len = ReadUShort(binaryForm[2..]);
        if (len > binaryForm.Length)
        {
            throw new ArgumentException("Invalid ACE - truncated", nameof(binaryForm));
        }

        if (len < 8 + SecurityIdentifier.MinBinaryLength)
        {
            throw new ArgumentException("Invalid ACE", nameof(binaryForm));
        }

        AccessMask = ReadInt(binaryForm[4..]);
        SecurityIdentifier = new SecurityIdentifier(binaryForm[8..]);

        var opaqueLen = len - (8 + SecurityIdentifier.BinaryLength);
        if (opaqueLen > 0)
        {
            var opaque = binaryForm.Slice(8 + SecurityIdentifier.BinaryLength, opaqueLen);
            SetOpaque(opaque);
        }
    }
#endif

    public override void GetBinaryForm(byte[] binaryForm, int offset)
    {
        var len = BinaryLength;
        binaryForm[offset] = (byte)AceType;
        binaryForm[offset + 1] = (byte)AceFlags;
        WriteUShort((ushort)len, binaryForm, offset + 2);
        WriteInt(AccessMask, binaryForm, offset + 4);

        SecurityIdentifier.GetBinaryForm(binaryForm,
            offset + 8);

        var opaque = GetOpaque();
        if (opaque != null)
        {
            Array.Copy(opaque, 0, binaryForm,
                offset + 8 + SecurityIdentifier.BinaryLength,
                opaque.Length);
        }
    }

    public static int MaxOpaqueLength(bool isCallback)
    {
        // Varies by platform?
        return 65459;
    }

    internal override string GetSddlForm()
    {
        if (OpaqueLength != 0)
        {
            throw new NotImplementedException(
                "Unable to convert conditional ACEs to SDDL");
        }

        return string.Format(CultureInfo.InvariantCulture,
            "({0};{1};{2};;;{3})",
            GetSddlAceType(AceType),
            GetSddlAceFlags(AceFlags),
            GetSddlAccessRights(AccessMask),
            SecurityIdentifier.GetSddlForm());
    }

    private static AceType ConvertType(AceQualifier qualifier,
                                       bool isCallback)
    {
        switch (qualifier)
        {
            case AceQualifier.AccessAllowed:
                if (isCallback)
                {
                    return AceType.AccessAllowedCallback;
                }
                else
                {
                    return AceType.AccessAllowed;
                }

            case AceQualifier.AccessDenied:
                if (isCallback)
                {
                    return AceType.AccessDeniedCallback;
                }
                else
                {
                    return AceType.AccessDenied;
                }

            case AceQualifier.SystemAlarm:
                if (isCallback)
                {
                    return AceType.SystemAlarmCallback;
                }
                else
                {
                    return AceType.SystemAlarm;
                }

            case AceQualifier.SystemAudit:
                if (isCallback)
                {
                    return AceType.SystemAuditCallback;
                }
                else
                {
                    return AceType.SystemAudit;
                }

            default:
                throw new ArgumentException("Unrecognized ACE qualifier: " + qualifier, nameof(qualifier));
        }
    }
}