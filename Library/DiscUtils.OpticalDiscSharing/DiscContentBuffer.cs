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

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DiscUtils.CoreCompat;
using DiscUtils.Streams;
using Buffer=DiscUtils.Streams.Buffer;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable SYSLIB0014 // Type or member is obsolete

namespace DiscUtils.OpticalDiscSharing;

internal sealed class DiscContentBuffer : Buffer
{
    private string _authHeader;

    private readonly string _password;
    private readonly Uri _uri;
    private readonly string _userName;

    internal DiscContentBuffer(Uri uri, string userName, string password)
    {
        _uri = uri;
        _userName = userName;
        _password = password;

        var response = SendRequest(() =>
        {
            var wr = (HttpWebRequest)WebRequest.Create(uri);
            wr.Method = "HEAD";
            return wr;
        });

        Capacity = response.ContentLength;
    }

    public override bool CanRead
    {
        get { return true; }
    }

    public override bool CanWrite
    {
        get { return false; }
    }

    public override long Capacity { get; }

    public override int Read(long pos, byte[] buffer, int offset, int count)
    {
        var response = SendRequest(() =>
        {
            var wr = (HttpWebRequest)WebRequest.Create(_uri);
            wr.Method = "GET";
            wr.AddRange((int)pos, (int)(pos + count - 1));
            return wr;
        });

        using var s = response.GetResponseStream();
        var total = (int)response.ContentLength;
        var read = 0;
        while (read < Math.Min(total, count))
        {
            read += s.Read(buffer, offset + read, count - read);
        }

        return read;
    }

#if NET45_OR_GREATER || NETSTANDARD || NETCOREAPP
    public override async Task<int> ReadAsync(long pos, byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var response = await SendRequestAsync(() =>
        {
            var wr = (HttpWebRequest)WebRequest.Create(_uri);
            wr.Method = "GET";
            wr.AddRange((int)pos, (int)(pos + count - 1));
            return wr;
        }).ConfigureAwait(false);

        using var s = response.GetResponseStream();
        var total = (int)response.ContentLength;
        var read = 0;
        while (read < Math.Min(total, count))
        {
            read += await s.ReadAsync(buffer, offset + read, count - read).ConfigureAwait(false);
        }

        return read;
    }
#endif

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override async ValueTask<int> ReadAsync(long pos, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var response = await SendRequestAsync(() =>
        {
            var wr = (HttpWebRequest)WebRequest.Create(_uri);
            wr.Method = "GET";
            wr.AddRange((int)pos, (int)(pos + buffer.Length - 1));
            return wr;
        }).ConfigureAwait(false);

        using var s = response.GetResponseStream();
        var total = (int)response.ContentLength;
        var read = 0;
        while (read < Math.Min(total, buffer.Length))
        {
            read += await s.ReadAsync(buffer[read..], cancellationToken).ConfigureAwait(false);
        }

        return read;
    }

    public override int Read(long pos, Span<byte> buffer)
    {
        var count = buffer.Length;

        var response = SendRequest(() =>
        {
            var wr = (HttpWebRequest)WebRequest.Create(_uri);
            wr.Method = "GET";
            wr.AddRange((int)pos, (int)(pos + count - 1));
            return wr;
        });

        using var s = response.GetResponseStream();
        var total = (int)response.ContentLength;
        var read = 0;
        while (read < Math.Min(total, buffer.Length))
        {
            read += s.Read(buffer[read..]);
        }

        return read;
    }
#endif

    public override void Write(long pos, byte[] buffer, int offset, int count)
    {
        throw new InvalidOperationException("Attempt to write to shared optical disc");
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP
    public override void Write(long pos, ReadOnlySpan<byte> buffer) =>
        throw new InvalidOperationException("Attempt to write to shared optical disc");
#endif

    public override void SetCapacity(long value)
    {
        throw new InvalidOperationException("Attempt to change size of shared optical disc");
    }

    public override IEnumerable<StreamExtent> GetExtentsInRange(long start, long count)
    {
        return StreamExtent.Intersect(
            new[] { new StreamExtent(0, Capacity) },
            new StreamExtent(start, count));
    }

    private static string ToHexString(byte[] p)
    {
        var result = new StringBuilder();

        for (var i = 0; i < p.Length; ++i)
        {
            var j = (p[i] >> 4) & 0xf;
            result.Append((char)(j <= 9 ? '0' + j : 'a' + (j - 10)));
            j = p[i] & 0xf;
            result.Append((char)(j <= 9 ? '0' + j : 'a' + (j - 10)));
        }

        return result.ToString();
    }

    private static Dictionary<string, string> ParseAuthenticationHeader(string header, out string authMethod)
    {
        var result = new Dictionary<string, string>();

        var elements = header.Split(' ');

        authMethod = elements[0];

        for (var i = 1; i < elements.Length; ++i)
        {
            var nvPair = elements[i].Split('=', 2, StringSplitOptions.None);
            result.Add(nvPair[0], nvPair[1].Trim('\"'));
        }

        return result;
    }

    private HttpWebResponse SendRequest(WebRequestCreator wrc)
    {
        var wr = wrc();
        if (_authHeader != null)
        {
            wr.Headers["Authorization"] = _authHeader;
        }

        try
        {
            return (HttpWebResponse)wr.GetResponse();
        }
        catch (WebException we)
        {
            var wresp = (HttpWebResponse)we.Response;

            if (wresp.StatusCode == HttpStatusCode.Unauthorized)
            {
                var authParams = ParseAuthenticationHeader(wresp.Headers["WWW-Authenticate"], out var authMethod);

                if (authMethod != "Digest")
                {
                    throw;
                }

                var resp = CalcDigestResponse(authParams["nonce"], wr.RequestUri.AbsolutePath, wr.Method, authParams["realm"]);

                _authHeader = "Digest username=\"" + _userName + "\", realm=\"ODS\", nonce=\"" + authParams["nonce"] + "\", uri=\"" + wr.RequestUri.AbsolutePath + "\", response=\"" + resp + "\"";

                (wresp as IDisposable).Dispose();

                wr = wrc();
                wr.Headers["Authorization"] = _authHeader;

                return (HttpWebResponse)wr.GetResponse();
            }

            throw;
        }
    }

#if NET45_OR_GREATER || NETSTANDARD || NETCOREAPP
    private async Task<HttpWebResponse> SendRequestAsync(WebRequestCreator wrc)
    {
        var wr = wrc();
        if (_authHeader != null)
        {
            wr.Headers["Authorization"] = _authHeader;
        }

        try
        {
            return (HttpWebResponse)await wr.GetResponseAsync().ConfigureAwait(false);
        }
        catch (WebException we)
        {
            var wresp = (HttpWebResponse)we.Response;

            if (wresp.StatusCode == HttpStatusCode.Unauthorized)
            {
                var authParams = ParseAuthenticationHeader(wresp.Headers["WWW-Authenticate"], out var authMethod);

                if (authMethod != "Digest")
                {
                    throw;
                }

                var resp = CalcDigestResponse(authParams["nonce"], wr.RequestUri.AbsolutePath, wr.Method, authParams["realm"]);

                _authHeader = "Digest username=\"" + _userName + "\", realm=\"ODS\", nonce=\"" + authParams["nonce"] + "\", uri=\"" + wr.RequestUri.AbsolutePath + "\", response=\"" + resp + "\"";

                (wresp as IDisposable).Dispose();

                wr = wrc();
                wr.Headers["Authorization"] = _authHeader;

                return (HttpWebResponse)await wr.GetResponseAsync().ConfigureAwait(false);
            }

            throw;
        }
    }
#endif

    private string CalcDigestResponse(string nonce, string uriPath, string method, string realm)
    {
        var a2 = method + ":" + uriPath;
        var ha2hash = MD5.Create();
        var ha2 = ToHexString(ha2hash.ComputeHash(Encoding.ASCII.GetBytes(a2)));

        var a1 = _userName + ":" + realm + ":" + _password;
        var ha1hash = MD5.Create();
        var ha1 = ToHexString(ha1hash.ComputeHash(Encoding.ASCII.GetBytes(a1)));

        var toHash = ha1 + ":" + nonce + ":" + ha2;
        var respHas = MD5.Create();
        var hash = respHas.ComputeHash(Encoding.ASCII.GetBytes(toHash));
        return ToHexString(hash);
    }

    internal delegate HttpWebRequest WebRequestCreator();
}