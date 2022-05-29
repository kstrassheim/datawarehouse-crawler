using DatawarehouseCrawler.DataAdapters;
using DatawarehouseCrawler.IntegrationTest.Helper;
using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.Providers.DataSetProviders;
using DatawarehouseCrawler.Providers.FileStreamProviders;
using DatawarehouseCrawler.QueryAdapters;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Data;

namespace DatawarehouseCrawler.IntegrationTest
{
    [TestClass]
    public class CsvImporterTest : AbstractDataSetImporterTest
    {
        protected override Dictionary<string, IDataSetProvider> providers { get; } = new Dictionary<string, IDataSetProvider> {
            { MockHelper.GetDefaultExcelImportSingleIdTable().SourceName, new CsvDataSetProvider(new LocalFileStreamProvider(FileHelper.MockupCsvFile), MockHelper.GetDefaultExcelImportSingleIdTable().SourceName) }
        };
    }
}
