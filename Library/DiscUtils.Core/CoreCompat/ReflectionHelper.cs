using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;

namespace DiscUtils.CoreCompat
{
    internal static class ReflectionHelper
    {
#if NETFRAMEWORK && !NET451_OR_GREATER
        public static int SizeOf<T>() => Marshal.SizeOf(typeof(T));
#else
        public static int SizeOf<T>() => Marshal.SizeOf<T>();
#endif

#if NETFRAMEWORK && !NET45_OR_GREATER
        public static T GetCustomAttribute<T>(this MemberInfo memberInfo) where T : Attribute =>
            Attribute.GetCustomAttribute(memberInfo, typeof(T)) as T;

        public static T GetCustomAttribute<T>(this MemberInfo memberInfo, bool inherit) where T : Attribute =>
            Attribute.GetCustomAttribute(memberInfo, typeof(T), inherit) as T;

        public static T[] GetCustomAttributes<T>(this MemberInfo memberInfo) where T : Attribute =>
            Attribute.GetCustomAttributes(memberInfo, typeof(T)) as T[];

        public static T[] GetCustomAttributes<T>(this MemberInfo memberInfo, bool inherit) where T : Attribute =>
            Attribute.GetCustomAttributes(memberInfo, typeof(T), inherit) as T[];
#endif
    }
}