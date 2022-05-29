using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DatawarehouseCrawler.Providers.DataSetProviders
{
    public interface IDataSetProvider :IDisposable
    {
        TimeSpan InitProcessTime { get; }
        string TableName { get; }
        DataSet DataSet { get; }
        void Init();
    }
}
