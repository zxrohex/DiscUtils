using System;
using System.Globalization;
using System.Text;

namespace DiscUtils.Core.WindowsSecurity.AccessControl
{
    public abstract class GenericAce
    {
        public AceFlags AceFlags { get; set; }

        public AceType AceType { get; }

        public AuditFlags AuditFlags
        {
            get
            {
                var ret = AuditFlags.None;
                if ((AceFlags & AceFlags.SuccessfulAccess) != 0)
                {
                    ret |= AuditFlags.Success;
                }

                if ((AceFlags & AceFlags.FailedAccess) != 0)
                {
                    ret |= AuditFlags.Failure;
                }

                return ret;
            }
        }

        public abstract int BinaryLength { get; }

        public InheritanceFlags InheritanceFlags
        {
            get
            {
                var ret = InheritanceFlags.None;
                if ((AceFlags & AceFlags.ObjectInherit) != 0)
                {
                    ret |= InheritanceFlags.ObjectInherit;
                }

                if ((AceFlags & AceFlags.ContainerInherit) != 0)
                {
                    ret |= InheritanceFlags.ContainerInherit;
                }

                return ret;
            }
        }

        public bool IsInherited => (AceFlags & AceFlags.Inherited) != AceFlags.None;

        public PropagationFlags PropagationFlags
        {
            get
            {
                var ret = PropagationFlags.None;
                if ((AceFlags & AceFlags.InheritOnly) != 0)
                {
                    ret |= PropagationFlags.InheritOnly;
                }

                if ((AceFlags & AceFlags.NoPropagateInherit) != 0)
                {
                    ret |= PropagationFlags.NoPropagateInherit;
                }

                return ret;
            }
        }

        internal GenericAce(AceType type, AceFlags flags)
        {
            if (type > AceType.MaxDefinedAceType)
            {
                throw new ArgumentOutOfRangeException(nameof(type));
            }

            AceType = type;
            AceFlags = flags;
        }

        internal GenericAce(byte[] binaryForm, int offset)
        {
            if (binaryForm == null)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (offset < 0 || offset > binaryForm.Length - 2)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset out of range");
            }

            AceType = (AceType)binaryForm[offset];
            AceFlags = (AceFlags)binaryForm[offset + 1];
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        internal GenericAce(ReadOnlySpan<byte> binaryForm)
        {
            if (binaryForm.IsEmpty)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (2 > binaryForm.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(binaryForm), binaryForm.Length, "Binary length out of range");
            }

            AceType = (AceType)binaryForm[0];
            AceFlags = (AceFlags)binaryForm[1];
        }
#endif

        public GenericAce Copy()
        {
            var buffer = new byte[BinaryLength];
            GetBinaryForm(buffer, 0);
            return CreateFromBinaryForm(buffer, 0);
        }

        public static GenericAce CreateFromBinaryForm(byte[] binaryForm, int offset)
        {
            if (binaryForm == null)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (offset < 0 || offset > binaryForm.Length - 1)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset out of range");
            }

            var type = (AceType)binaryForm[offset];
            if (IsObjectType(type))
            {
                return new ObjectAce(binaryForm, offset);
            }
            else
            {
                return new CommonAce(binaryForm, offset);
            }
        }

        public static bool TryCreateFromBinaryForm(byte[] binaryForm, int offset, out GenericAce genericAce)
        {
            genericAce = null;

            if (binaryForm == null || offset < 0 || offset > binaryForm.Length - 1)
            {
                return false;
            }

            try
            {
                var type = (AceType)binaryForm[offset];
                if (IsObjectType(type))
                {
                    genericAce = new ObjectAce(binaryForm, offset);
                }
                else
                {
                    genericAce = new CommonAce(binaryForm, offset);
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        public static GenericAce CreateFromBinaryForm(ReadOnlySpan<byte> binaryForm)
        {
            if (binaryForm == null)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (1 > binaryForm.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(binaryForm), binaryForm.Length, "Binary length out of range");
            }

            var type = (AceType)binaryForm[0];
            if (IsObjectType(type))
            {
                return new ObjectAce(binaryForm);
            }
            else
            {
                return new CommonAce(binaryForm);
            }
        }

        public static bool TryCreateFromBinaryForm(ReadOnlySpan<byte> binaryForm, out GenericAce genericAce)
        {
            genericAce = null;

            if (binaryForm == null || 1 > binaryForm.Length)
            {
                return false;
            }

            try
            {
                var type = (AceType)binaryForm[0];
                if (IsObjectType(type))
                {
                    genericAce = new ObjectAce(binaryForm);
                }
                else
                {
                    genericAce = new CommonAce(binaryForm);
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

#endif

        public sealed override bool Equals(object o) => this == (o as GenericAce);

        public abstract void GetBinaryForm(byte[] binaryForm, int offset);

        public sealed override int GetHashCode()
        {
            var buffer = new byte[BinaryLength];
            GetBinaryForm(buffer, 0);

            var code = 0;
            for (var i = 0; i < buffer.Length; ++i)
            {
                code = (code << 3) | ((code >> 29) & 0x7);
                code ^= ((int)buffer[i]) & 0xff;
            }

            return code;
        }

        public static bool operator ==(GenericAce left, GenericAce right)
        {
            if (((object)left) == null)
            {
                return ((object)right) == null;
            }

            if (((object)right) == null)
            {
                return false;
            }

            var leftLen = left.BinaryLength;
            var rightLen = right.BinaryLength;
            if (leftLen != rightLen)
            {
                return false;
            }

            var leftBuffer = new byte[leftLen];
            var rightBuffer = new byte[rightLen];
            left.GetBinaryForm(leftBuffer, 0);
            right.GetBinaryForm(rightBuffer, 0);

            for (var i = 0; i < leftLen; ++i)
            {
                if (leftBuffer[i] != rightBuffer[i])
                {
                    return false;
                }
            }

            return true;
        }

        public static bool operator !=(GenericAce left, GenericAce right)
        {
            if (((object)left) == null)
            {
                return ((object)right) != null;
            }

            if (((object)right) == null)
            {
                return true;
            }

            var leftLen = left.BinaryLength;
            var rightLen = right.BinaryLength;
            if (leftLen != rightLen)
            {
                return true;
            }

            var leftBuffer = new byte[leftLen];
            var rightBuffer = new byte[rightLen];
            left.GetBinaryForm(leftBuffer, 0);
            right.GetBinaryForm(rightBuffer, 0);

            for (var i = 0; i < leftLen; ++i)
            {
                if (leftBuffer[i] != rightBuffer[i])
                {
                    return true;
                }
            }

            return false;
        }

        internal abstract string GetSddlForm();

        internal static GenericAce CreateFromSddlForm(string sddlForm, ref int pos)
        {
            if (sddlForm[pos] != '(')
            {
                throw new ArgumentException("Invalid SDDL string.", nameof(sddlForm));
            }

            var endPos = sddlForm.IndexOf(')', pos);
            if (endPos < 0)
            {
                throw new ArgumentException("Invalid SDDL string.", nameof(sddlForm));
            }

            var count = endPos - (pos + 1);
            var elementsStr = sddlForm.Substring(pos + 1,
                count);
            elementsStr = elementsStr.ToUpperInvariant();
            var elements = elementsStr.Split(';');
            if (elements.Length != 6)
            {
                throw new ArgumentException("Invalid SDDL string.", nameof(sddlForm));
            }

            var objFlags = ObjectAceFlags.None;

            var type = ParseSddlAceType(elements[0]);

            var flags = ParseSddlAceFlags(elements[1]);

            var accessMask = ParseSddlAccessRights(elements[2]);

            var objectType = Guid.Empty;
            if (!string.IsNullOrEmpty(elements[3]))
            {
                objectType = new Guid(elements[3]);
                objFlags |= ObjectAceFlags.ObjectAceTypePresent;
            }

            var inhObjectType = Guid.Empty;
            if (!string.IsNullOrEmpty(elements[4]))
            {
                inhObjectType = new Guid(elements[4]);
                objFlags |= ObjectAceFlags.InheritedObjectAceTypePresent;
            }

            var sid
                = new SecurityIdentifier(elements[5]);

            if (type is AceType.AccessAllowedCallback
                or AceType.AccessDeniedCallback)
            {
                throw new NotImplementedException("Conditional ACEs not supported");
            }

            pos = endPos + 1;

            if (IsObjectType(type))
            {
                return new ObjectAce(type, flags, accessMask, sid, objFlags, objectType, inhObjectType, null);
            }
            else
            {
                if (objFlags != ObjectAceFlags.None)
                {
                    throw new ArgumentException("Invalid SDDL string.", nameof(sddlForm));
                }

                return new CommonAce(type, flags, accessMask, sid, null);
            }
        }

        private static bool IsObjectType(AceType type)
        {
            return type is AceType.AccessAllowedCallbackObject
                   or AceType.AccessAllowedObject
                   or AceType.AccessDeniedCallbackObject
                   or AceType.AccessDeniedObject
                   or AceType.SystemAlarmCallbackObject
                   or AceType.SystemAlarmObject
                   or AceType.SystemAuditCallbackObject
                   or AceType.SystemAuditObject;
        }

        internal static string GetSddlAceType(AceType type)
        {
            return type switch
            {
                AceType.AccessAllowed => "A",
                AceType.AccessDenied => "D",
                AceType.AccessAllowedObject => "OA",
                AceType.AccessDeniedObject => "OD",
                AceType.SystemAudit => "AU",
                AceType.SystemAlarm => "AL",
                AceType.SystemAuditObject => "OU",
                AceType.SystemAlarmObject => "OL",
                AceType.AccessAllowedCallback => "XA",
                AceType.AccessDeniedCallback => "XD",
                _ => throw new ArgumentException($"Unable to convert to SDDL ACE type: {type}", nameof(type)),
            };
        }

        private static AceType ParseSddlAceType(string type)
        {
            return type switch
            {
                "A" => AceType.AccessAllowed,
                "D" => AceType.AccessDenied,
                "OA" => AceType.AccessAllowedObject,
                "OD" => AceType.AccessDeniedObject,
                "AU" => AceType.SystemAudit,
                "AL" => AceType.SystemAlarm,
                "OU" => AceType.SystemAuditObject,
                "OL" => AceType.SystemAlarmObject,
                "XA" => AceType.AccessAllowedCallback,
                "XD" => AceType.AccessDeniedCallback,
                _ => throw new ArgumentException($"Unable to convert SDDL to ACE type: {type}", nameof(type)),
            };
        }

        internal static string GetSddlAceFlags(AceFlags flags)
        {
            var result = new StringBuilder();
            if ((flags & AceFlags.ObjectInherit) != 0)
            {
                result.Append("OI");
            }

            if ((flags & AceFlags.ContainerInherit) != 0)
            {
                result.Append("CI");
            }

            if ((flags & AceFlags.NoPropagateInherit) != 0)
            {
                result.Append("NP");
            }

            if ((flags & AceFlags.InheritOnly) != 0)
            {
                result.Append("IO");
            }

            if ((flags & AceFlags.Inherited) != 0)
            {
                result.Append("ID");
            }

            if ((flags & AceFlags.SuccessfulAccess) != 0)
            {
                result.Append("SA");
            }

            if ((flags & AceFlags.FailedAccess) != 0)
            {
                result.Append("FA");
            }

            return result.ToString();
        }

        private static AceFlags ParseSddlAceFlags(string flags)
        {
            var ret = AceFlags.None;

            var pos = 0;
            while (pos < flags.Length - 1)
            {
                var flag = flags.Substring(pos, 2);
                ret |= flag switch
                {
                    "CI" => AceFlags.ContainerInherit,
                    "OI" => AceFlags.ObjectInherit,
                    "NP" => AceFlags.NoPropagateInherit,
                    "IO" => AceFlags.InheritOnly,
                    "ID" => AceFlags.Inherited,
                    "SA" => AceFlags.SuccessfulAccess,
                    "FA" => AceFlags.FailedAccess,
                    _ => throw new ArgumentException("Invalid SDDL string.", nameof(flags)),
                };
                pos += 2;
            }

            if (pos != flags.Length)
            {
                throw new ArgumentException("Invalid SDDL string.", nameof(flags));
            }

            return ret;
        }

        private static int ParseSddlAccessRights(string accessMask)
        {
            if (accessMask.StartsWith("0X"))
            {
                return int.Parse(accessMask.Substring(2),
                    NumberStyles.HexNumber,
                    CultureInfo.InvariantCulture);
            }
            else if (char.IsDigit(accessMask, 0))
            {
                return int.Parse(accessMask,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture);
            }
            else
            {
                return ParseSddlAliasRights(accessMask);
            }
        }

        private static int ParseSddlAliasRights(string accessMask)
        {
            var ret = 0;

            var pos = 0;
            while (pos < accessMask.Length - 1)
            {
                var flag = accessMask.Substring(pos, 2);
                var right = SddlAccessRight.LookupByName(flag);
                if (right == null)
                {
                    throw new ArgumentException("Invalid SDDL string.", nameof(accessMask));
                }

                ret |= right.Value;
                pos += 2;
            }

            if (pos != accessMask.Length)
            {
                throw new ArgumentException("Invalid SDDL string.", nameof(accessMask));
            }

            return ret;
        }

        internal static ushort ReadUShort(byte[] buffer, int offset)
        {
            return (ushort)((((int)buffer[offset + 0]) << 0)
                            | (((int)buffer[offset + 1]) << 8));
        }

        internal static int ReadInt(byte[] buffer, int offset)
        {
            return (((int)buffer[offset + 0]) << 0)
                   | (((int)buffer[offset + 1]) << 8)
                   | (((int)buffer[offset + 2]) << 16)
                   | (((int)buffer[offset + 3]) << 24);
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        internal static ushort ReadUShort(ReadOnlySpan<byte> buffer)
        {
            return (ushort)((((int)buffer[0]) << 0)
                            | (((int)buffer[1]) << 8));
        }

        internal static int ReadInt(ReadOnlySpan<byte> buffer)
        {
            return (((int)buffer[0]) << 0)
                   | (((int)buffer[1]) << 8)
                   | (((int)buffer[2]) << 16)
                   | (((int)buffer[3]) << 24);
        }
#endif

        internal static void WriteInt(int val, byte[] buffer, int offset)
        {
            buffer[offset] = (byte)val;
            buffer[offset + 1] = (byte)(val >> 8);
            buffer[offset + 2] = (byte)(val >> 16);
            buffer[offset + 3] = (byte)(val >> 24);
        }

        internal static void WriteUShort(ushort val, byte[] buffer,
                                         int offset)
        {
            buffer[offset] = (byte)val;
            buffer[offset + 1] = (byte)(val >> 8);
        }
    }
}