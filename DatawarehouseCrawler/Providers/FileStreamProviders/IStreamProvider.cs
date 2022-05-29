using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DatawarehouseCrawler.Providers.FileStreamProviders
{
    public interface IStreamProvider
    {
        Stream GetStream();

        bool StreamExists();
    }
}
