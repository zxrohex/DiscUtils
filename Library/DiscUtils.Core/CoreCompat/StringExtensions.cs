#if NETSTANDARD1_5
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace DiscUtils.CoreCompat
{
    internal static class StringExtensions
    {
        public static string ToUpper(this string value, CultureInfo _) =>
            value.ToUpper();

        public static List<TResult> ConvertAll<TSource, TResult>(this List<TSource> list, Func<TSource, TResult> converter)
        {
            var newlist = new List<TResult>(list.Count);
            newlist.AddRange(list.Select(converter));
            return newlist;
        }
    }
}

#endif