using System;
using System.Globalization;
using System.Text;

namespace DiscUtils.Core.WindowsSecurity.AccessControl
{
    public abstract class KnownAce : GenericAce
    {
        public int AccessMask { get; set; }
        public SecurityIdentifier SecurityIdentifier { get; set; }
        
        internal KnownAce(AceType type, AceFlags flags)
            : base(type, flags) { }

        internal KnownAce(byte[] binaryForm, int offset)
            : base(binaryForm, offset) { }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
        internal KnownAce(ReadOnlySpan<byte> binaryForm)
            : base(binaryForm) { }
#endif

        internal static string GetSddlAccessRights(int accessMask)
        {
            var ret = GetSddlAliasRights(accessMask);
            return !string.IsNullOrEmpty(ret)
                ? ret
                : string.Format(CultureInfo.InvariantCulture,
                "0x{0:x}", accessMask);
        }

        private static string GetSddlAliasRights(int accessMask)
        {
            var rights = SddlAccessRight.Decompose(accessMask);
            if (rights == null)
            {
                return null;
            }

            var ret = new StringBuilder();
            foreach (var right in rights)
            {
                ret.Append(right.Name);
            }

            return ret.ToString();
        }
    }
}