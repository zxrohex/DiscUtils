//
// Copyright (c) 2008-2011, Kenneth Bell
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//

using System.IO;
using System.Management.Automation;

namespace DiscUtils.PowerShell;

internal class Utilities
{
    /// <summary>
    /// Replace all ':' characters with '#'.
    /// </summary>
    /// <param name="path">The path to normalize</param>
    /// <returns>The normalized path</returns>
    /// <remarks>
    /// PowerShell has a bug that prevents tab-completion if the paths contain ':'
    /// characters, so in the external path for this provider we encode ':' as '#'.
    /// </remarks>
    public static string NormalizePath(string path)
    {
        return path.Replace(':', '#');
    }

    /// <summary>
    /// Replace all '#' characters with ':'.
    /// </summary>
    /// <param name="path">The path to normalize</param>
    /// <returns>The normalized path</returns>
    /// <remarks>
    /// PowerShell has a bug that prevents tab-completion if the paths contain ':'
    /// characters, so in the external path for this provider we encode ':' as '#'.
    /// </remarks>
    public static string DenormalizePath(string path)
    {
        return path.Replace('#', ':');
    }

    public static Stream CreatePsPath(SessionState session, string filePath)
    {
        var parentPath = session.Path.ParseParent(filePath, null);
        var childName = session.Path.ParseChildName(filePath);
        var parentItems = session.InvokeProvider.Item.Get(parentPath);
        if (parentItems.Count > 1)
        {
            throw new IOException($"PowerShell path {parentPath} is ambiguous");
        }
        else if (parentItems.Count < 1)
        {
            throw new DirectoryNotFoundException("No such directory");
        }

        if (parentItems[0].BaseObject is DirectoryInfo parentAsDir)
        {
            return File.Create(Path.Combine(parentAsDir.FullName, childName));
        }

        if (parentItems[0].BaseObject is DiscDirectoryInfo parentAsDiscDir)
        {
            return parentAsDiscDir.FileSystem.OpenFile(Path.Combine(parentAsDiscDir.FullName, childName), FileMode.Create, FileAccess.ReadWrite);
        }

        throw new FileNotFoundException("Path is not a directory", parentPath);
    }

    public static Stream OpenPsPath(SessionState session, string filePath, FileAccess access, FileShare share)
    {
        var items = session.InvokeProvider.Item.Get(filePath);
        if (items.Count == 1)
        {
            if (items[0].BaseObject is FileInfo itemAsFile)
            {
                return itemAsFile.Open(FileMode.Open, access, share);
            }

            if (items[0].BaseObject is DiscFileInfo itemAsDiscFile)
            {
                return itemAsDiscFile.Open(FileMode.Open, access);
            }

            throw new FileNotFoundException("Path is not a file", filePath);
        }
        else if (items.Count > 1)
        {
            throw new IOException($"PowerShell path {filePath} is ambiguous");
        }
        else
        {
            throw new FileNotFoundException("No such file", filePath);
        }
    }

    public static string ResolvePsPath(SessionState session, string filePath)
    {
        var paths = session.Path.GetResolvedPSPathFromPSPath(filePath);
        if (paths.Count > 1)
        {
            throw new IOException($"PowerShell path {filePath} is ambiguous");
        }
        else if (paths.Count < 1)
        {
            throw new IOException($"PowerShell path {filePath} not found");
        }

        return paths[0].Path;
    }

}
