using DiscUtils.Core.WindowsSecurity;
using DiscUtils.Core.WindowsSecurity.AccessControl;
using System;

namespace DiscUtils.Registry;

public class RegistrySecurity : RawSecurityDescriptor
{
    public RegistrySecurity(string sddlForm) : base(sddlForm)
    {
    }

    public RegistrySecurity(ReadOnlySpan<byte> binaryForm) : base(binaryForm)
    {
    }

    public RegistrySecurity(ControlFlags flags, SecurityIdentifier owner, SecurityIdentifier group, RawAcl systemAcl, RawAcl discretionaryAcl) : base(flags, owner, group, systemAcl, discretionaryAcl)
    {
    }
}
