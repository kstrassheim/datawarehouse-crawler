using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace DatawarehouseCrawler.Providers.FileStreamProviders
{

    public class FtpFileStreamProvider : IStreamProvider
    {
        protected string Url { get; set; }

        protected string Username { get; set; }

        protected string Password { get; set; }

        public Stream GetStream()
        {
            WebClient rq = new WebClient(); 
            rq.Credentials = new NetworkCredential(this.Username, this.Password);
            return new MemoryStream(rq.DownloadData(this.Url));
        }

        public bool StreamExists()
        {
            try
            {
                WebClient rq = new WebClient();
                rq.Credentials = new NetworkCredential(this.Username, this.Password);
                var ms = new MemoryStream(rq.DownloadData(this.Url));
                return ms.Length > 0;
            }
            catch
            {
                return false;
            }
        }

        public FtpFileStreamProvider(string url, string username, string password)
        {
            this.Url = url;
            this.Username = username;
            this.Password = password;
        }
    }
}
