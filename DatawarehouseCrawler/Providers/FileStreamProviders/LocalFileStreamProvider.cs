using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DatawarehouseCrawler.Providers.FileStreamProviders
{
    public class LocalFileStreamProvider: IStreamProvider
    {
        protected string FileName { get; set; }

        public Stream GetStream()
        {
            return new FileStream(this.FileName, FileMode.Open, FileAccess.Read);
        }
        public bool StreamExists()
        {
            return File.Exists(this.FileName);
        }

        public LocalFileStreamProvider(string filename)
        {
            this.FileName = filename;
        }
    }
}
