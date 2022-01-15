using System;
using System.Collections.Generic;
using System.Text;

namespace DiscUtils.Core.WindowsSecurity.AccessControl
{
    public sealed class RawAcl : GenericAcl
    {
        private readonly byte _revision;
        private readonly List<GenericAce> _list;

        public RawAcl(byte revision, int capacity)
        {
            _revision = revision;
            _list = new List<GenericAce>(capacity);
        }

        public RawAcl(byte[] binaryForm, int offset)
        {
            if (binaryForm == null)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (offset < 0 || offset > binaryForm.Length - 8)
            {
                throw new ArgumentOutOfRangeException(nameof(offset), offset, "Offset out of range");
            }

            _revision = binaryForm[offset];
            if (_revision != AclRevision && _revision != AclRevisionDS)
            {
                throw new ArgumentException("Invalid ACL - unknown revision", nameof(binaryForm));
            }

            int binaryLength = ReadUShort(binaryForm, offset + 2);
            if (offset > binaryForm.Length - binaryLength)
            {
                throw new ArgumentException("Invalid ACL - truncated", nameof(binaryForm));
            }

            var pos = offset + 8;
            int numAces = ReadUShort(binaryForm, offset + 4);
            _list = new List<GenericAce>(numAces);
            for (var i = 0; i < numAces; ++i)
            {
                var newAce = GenericAce.CreateFromBinaryForm(binaryForm, pos);
                _list.Add(newAce);
                pos += newAce.BinaryLength;
            }
        }

        private RawAcl() { }

        public static bool TryParse(byte[] binaryForm, int offset, out RawAcl rawAcl)
        {
            rawAcl = null;

            if (binaryForm == null || offset < 0 || offset > binaryForm.Length - 8)
            {
                return false;
            }

            var revision = binaryForm[offset];
            if (revision != AclRevision && revision != AclRevisionDS)
            {
                return false;
            }

            int binaryLength = ReadUShort(binaryForm, offset + 2);
            if (offset > binaryForm.Length - binaryLength)
            {
                return false;
            }

            var pos = offset + 8;
            int numAces = ReadUShort(binaryForm, offset + 4);
            var list = new List<GenericAce>(numAces);
            for (var i = 0; i < numAces; ++i)
            {
                if (!GenericAce.TryCreateFromBinaryForm(binaryForm, pos, out var newAce))
                {
                    return false;
                }
                list.Add(newAce);
                pos += newAce.BinaryLength;
            }

            rawAcl = new(revision, list);
            return true;
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        public RawAcl(ReadOnlySpan<byte> binaryForm)
        {
            if (binaryForm == null)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (8 > binaryForm.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(binaryForm), binaryForm.Length, "Binary length out of range");
            }

            _revision = binaryForm[0];
            if (_revision != AclRevision && _revision != AclRevisionDS)
            {
                throw new ArgumentException("Invalid ACL - unknown revision", nameof(binaryForm));
            }

            int binaryLength = ReadUShort(binaryForm[2..]);
            if (binaryLength > binaryForm.Length)
            {
                throw new ArgumentException("Invalid ACL - truncated", nameof(binaryForm));
            }

            var pos = 8;
            int numAces = ReadUShort(binaryForm[4..]);
            _list = new List<GenericAce>(numAces);
            for (var i = 0; i < numAces; ++i)
            {
                var newAce = GenericAce.CreateFromBinaryForm(binaryForm[pos..]);
                _list.Add(newAce);
                pos += newAce.BinaryLength;
            }
        }

        public static bool TryParse(ReadOnlySpan<byte> binaryForm, out RawAcl rawAcl)
        {
            rawAcl = null;

            if (binaryForm == null || 8 > binaryForm.Length)
            {
                return false;
            }

            var revision = binaryForm[0];
            if (revision != AclRevision && revision != AclRevisionDS)
            {
                return false;
            }

            int binaryLength = ReadUShort(binaryForm[2..]);
            if (binaryLength > binaryForm.Length)
            {
                return false;
            }

            var pos = 8;
            int numAces = ReadUShort(binaryForm[4..]);
            var list = new List<GenericAce>(numAces);
            for (var i = 0; i < numAces; ++i)
            {
                if (!GenericAce.TryCreateFromBinaryForm(binaryForm[pos..], out var newAce))
                {
                    return false;
                }
                list.Add(newAce);
                pos += newAce.BinaryLength;
            }

            rawAcl = new(revision, list);
            return true;
        }
#endif

        internal RawAcl(byte revision, List<GenericAce> aces)
        {
            _revision = revision;
            _list = aces;
        }

        public override int BinaryLength
        {
            get
            {
                var len = 8;
                foreach (var ace in _list)
                {
                    len += ace.BinaryLength;
                }
                return len;
            }
        }

        public override int Count => _list.Count;

        public override GenericAce this[int index]
        {
            get => _list[index];
            set => _list[index] = value;
        }

        public override byte Revision => _revision;

        public override void GetBinaryForm(byte[] binaryForm, int offset)
        {
            if (binaryForm == null)
            {
                throw new ArgumentNullException(nameof(binaryForm));
            }

            if (offset < 0
                || offset > binaryForm.Length - BinaryLength)
            {
                throw new ArgumentException("Offset out of range", nameof(offset));
            }

            binaryForm[offset] = Revision;
            binaryForm[offset + 1] = 0;
            WriteUShort((ushort)BinaryLength, binaryForm,
                offset + 2);
            WriteUShort((ushort)_list.Count, binaryForm,
                offset + 4);
            WriteUShort(0, binaryForm, offset + 6);

            var pos = offset + 8;
            foreach (var ace in _list)
            {
                ace.GetBinaryForm(binaryForm, pos);
                pos += ace.BinaryLength;
            }
        }

        public void InsertAce(int index, GenericAce ace)
        {
            if (ace == null)
            {
                throw new ArgumentNullException(nameof(ace));
            }

            _list.Insert(index, ace);
        }

        public void RemoveAce(int index) => _list.RemoveAt(index);

        internal override string GetSddlForm(ControlFlags sdFlags,
                                             bool isDacl)
        {
            var result = new StringBuilder();

            if (isDacl)
            {
                if ((sdFlags & ControlFlags.DiscretionaryAclProtected) != 0)
                {
                    result.Append("P");
                }

                if ((sdFlags & ControlFlags.DiscretionaryAclAutoInheritRequired) != 0)
                {
                    result.Append("AR");
                }

                if ((sdFlags & ControlFlags.DiscretionaryAclAutoInherited) != 0)
                {
                    result.Append("AI");
                }
            }
            else
            {
                if ((sdFlags & ControlFlags.SystemAclProtected) != 0)
                {
                    result.Append("P");
                }

                if ((sdFlags & ControlFlags.SystemAclAutoInheritRequired) != 0)
                {
                    result.Append("AR");
                }

                if ((sdFlags & ControlFlags.SystemAclAutoInherited) != 0)
                {
                    result.Append("AI");
                }
            }

            foreach (var ace in _list)
            {
                result.Append(ace.GetSddlForm());
            }

            return result.ToString();
        }

        internal static RawAcl ParseSddlForm(string sddlForm,
                                             bool isDacl,
                                             ref ControlFlags sdFlags,
                                             ref int pos)
        {
            ParseFlags(sddlForm, isDacl, ref sdFlags, ref pos);

            var revision = AclRevision;
            var aces = new List<GenericAce>();
            while (pos < sddlForm.Length && sddlForm[pos] == '(')
            {
                var ace = GenericAce.CreateFromSddlForm(
                    sddlForm, ref pos);
                if ((ace as ObjectAce) != null)
                {
                    revision = AclRevisionDS;
                }

                aces.Add(ace);
            }

            return new RawAcl(revision, aces);
        }

        private static void ParseFlags(string sddlForm,
                                       bool isDacl,
                                       ref ControlFlags sdFlags,
                                       ref int pos)
        {
            var ch = char.ToUpperInvariant(sddlForm[pos]);
            while (ch is 'P' or 'A')
            {
                if (ch == 'P')
                {
                    if (isDacl)
                    {
                        sdFlags |= ControlFlags.DiscretionaryAclProtected;
                    }
                    else
                    {
                        sdFlags |= ControlFlags.SystemAclProtected;
                    }

                    pos++;
                }
                else if (sddlForm.Length > pos + 1)
                {
                    ch = char.ToUpperInvariant(sddlForm[pos + 1]);
                    if (ch == 'R')
                    {
                        if (isDacl)
                        {
                            sdFlags |= ControlFlags.DiscretionaryAclAutoInheritRequired;
                        }
                        else
                        {
                            sdFlags |= ControlFlags.SystemAclAutoInheritRequired;
                        }

                        pos += 2;
                    }
                    else if (ch == 'I')
                    {
                        if (isDacl)
                        {
                            sdFlags |= ControlFlags.DiscretionaryAclAutoInherited;
                        }
                        else
                        {
                            sdFlags |= ControlFlags.SystemAclAutoInherited;
                        }

                        pos += 2;
                    }
                    else
                    {
                        throw new ArgumentException("Invalid SDDL string.", nameof(sddlForm));
                    }
                }
                else
                {
                    throw new ArgumentException("Invalid SDDL string.", nameof(sddlForm));
                }

                ch = char.ToUpperInvariant(sddlForm[pos]);
            }
        }

        private static void WriteUShort(ushort val, byte[] buffer, int offset)
        {
            buffer[offset] = (byte)val;
            buffer[offset + 1] = (byte)(val >> 8);
        }

        private static ushort ReadUShort(byte[] buffer, int offset)
        {
            return (ushort)((((int)buffer[offset + 0]) << 0)
                            | (((int)buffer[offset + 1]) << 8));
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        private static ushort ReadUShort(ReadOnlySpan<byte> buffer)
        {
            return (ushort)((((int)buffer[0]) << 0)
                            | (((int)buffer[1]) << 8));
        }
#endif
    }
}