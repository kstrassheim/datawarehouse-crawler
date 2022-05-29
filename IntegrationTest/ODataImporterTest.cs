using DatawarehouseCrawler;
using DatawarehouseCrawler.IntegrationTest.Helper;
using Newtonsoft.Json;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.IO;
using System.Text;
using System.Xml;
using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.QueryAdapters;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.DataAdapters;

namespace DatawarehouseCrawler.IntegrationTest
{
    [TestClass]
    public class ODataImporterTest
    {
        #region internal classes
        public class MockedODataQuery
        {
            public string Url { get; set; }
            public string Value { get; set; }
            public override string ToString() { return $"Url: {this.Url}; Value: {this.Value}"; }
            public override int GetHashCode() { return this.Url.GetHashCode(); }
        }

        public class MockedODataRequestProvider : ODataRequestProvider
        {
            public const string MOCK_FILE = "odata_mock.json";

            public List<MockedODataQuery> OverrideMockQueries { get; set; }

            public List<MockedODataQuery> MockQueries { get; set; }

            public MockedODataRequestProvider() { this.ReloadMockFile(); }

            private void ReloadMockFile()
            {
                this.OverrideMockQueries = new List<MockedODataQuery>();
                using (var sr = new StreamReader(MOCK_FILE)) { this.MockQueries = JsonConvert.DeserializeObject<MockedODataQuery[]>(sr.ReadToEnd())?.ToList(); }
            }

            public override Stream GetResponseFromServer(string url)
            {
                var query = this.OverrideMockQueries.FirstOrDefault(o => o.Url == url)?.Value;
                if (query == null) { query = this.MockQueries.FirstOrDefault(o => o.Url == url)?.Value; }
                #if (DEBUG)
                    // load url query from web and then save it to mock if in debug mode
                    if (query == null) {
                        var resp = base.GetResponseFromServer(url);
                        string value = string.Empty;
                        using (var sr = new StreamReader(resp)) { value = sr.ReadToEnd(); }
                        // save request to mock file
                        this.MockQueries.Add(new MockedODataQuery() { Url = url, Value = value });
                        string newMockFileContent = JsonConvert.SerializeObject(this.MockQueries.ToArray(), Newtonsoft.Json.Formatting.Indented);
                        using (var sw = new StreamWriter(MOCK_FILE, false)){ sw.Write(newMockFileContent); }
                        File.Copy(MOCK_FILE, $"..\\..\\..\\{MOCK_FILE}", true);
                        // reload mock file and query
                        this.ReloadMockFile();
                        query = this.MockQueries.FirstOrDefault(o => o.Url == url)?.Value;
                    }
                #endif
                if (query == null) throw new Exception($"OData query not found for url:{url}");
                var stream = new MemoryStream();
                var writer = new StreamWriter(stream);
                writer.Write(query);
                writer.Flush();
                stream.Position = 0;
                return stream;
            }
        }

        #endregion internal classes

        [TestInitialize()]
        public void Initialize()
        {
            SqlHelper.ConnectAndRestoreDatabase("Target");
        }

        private void InitAndTestImportTable(Model.ImportModel x, SqlConnection tgt, ODataRequestProvider mockedProvider = null, Action<DataImporter> options = null)
        {
            var sourceAdapter = new ODataAdapter(new Table(x.SourceName), ODataHelper.SourceConnectionString, x.QuerySubUrl);
            if (mockedProvider != null) { sourceAdapter.ODataQueryProvider = mockedProvider; }
            var targetAdapter = new DataAdapters.SqlDataAdapter(new Table(x.Name), tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter, ImportOperationMode.GenerateSchema);
            options?.Invoke(y);
            y.Run();
            var tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
            Assert.AreEqual(tableCreated, true, "table not created");
        }

        private DataImporter RunAndTestImporter(Model.ImportModel x, SqlConnection tgt, ODataRequestProvider mockedProvider = null, Action<DataImporter> options = null)
        {
            var sourceAdapter = new ODataAdapter(new Table(x.SourceName), ODataHelper.SourceConnectionString, x.QuerySubUrl);
            if (mockedProvider != null) { sourceAdapter.ODataQueryProvider = mockedProvider; }
            var targetAdapter = new DataAdapters.SqlDataAdapter(new Table(x.Name), tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter);
            
            options?.Invoke(y);
            y.Run();
            var sourceRowCount = y.CountStatus.SourceCount;
            var targetRowCount = SqlHelper.GetRowsCount(x.Name, tgt);
            Assert.AreEqual(sourceRowCount, targetRowCount, "target row count differs from source");
            return y;
        }

        [TestMethod]
        public void TestSchemaCopy()
        {
            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                var x = MockHelper.GetDefaultODataImportSingleIdTable();

                var sourceAdapter = new ODataAdapter(new Table(x.SourceName), ODataHelper.SourceConnectionString, x.QuerySubUrl);
                sourceAdapter.ODataQueryProvider = new MockedODataRequestProvider();
                var targetAdapter = new DataAdapters.SqlDataAdapter(new Table(x.Name), tgt);
                var p = new DataImporter(x, sourceAdapter, targetAdapter, ImportOperationMode.GenerateSchema);

                //var p = new ODataImporter(ODataHelper.SourceConnectionString, tgt, x, false, Abstract.AbstractImportProcessing.);
                //p.ODataQueryProvider = mock;
                p.Run();

                var tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
                Assert.AreEqual(tableCreated, true, "table not created");

                p.Force = true;
                p.Run();

                tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
                Assert.AreEqual(tableCreated, true, "table not created");

                // check primary key copy
                var srcPkeys = sourceAdapter.GetPrimaryKeys();
                var targetPkeys = SqlHelper.getPrimaryKeys(x.Name, tgt);

                Assert.AreEqual(srcPkeys.Length, targetPkeys.Length, "The length of the primary keys differs");
                CollectionAssert.AreEqual(srcPkeys, targetPkeys, "the primary keys were not copied correct");
            });
        }

        [TestMethod]
        public void TestDataCopy()
        {
            var x = MockHelper.GetDefaultODataImportSingleIdTable();
            var mock = new MockedODataRequestProvider();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt, mock);
                this.RunAndTestImporter(x, tgt, mock);
            });
        }

        [TestMethod]
        public void TestAppendById()
        {
            var x = MockHelper.GetDefaultODataImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbyid;
            var mock = new MockedODataRequestProvider();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values
                var m = new MockedODataQuery() { Url = "https://services.odata.org/V3/Northwind/Northwind.svc/Orders/$count", Value = "401" };
                mock.OverrideMockQueries.Add(m);
                this.RunAndTestImporter(x, tgt, mock);
                mock.OverrideMockQueries.Remove(m);
                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, mock);
            });
        }

        [TestMethod]
        public void TestAppendByDate()
        {
            var x = MockHelper.GetDefaultODataImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbydate;
            var mock = new MockedODataRequestProvider();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values
                var m = new MockedODataQuery() { Url = "https://services.odata.org/V3/Northwind/Northwind.svc/Orders/$count", Value = "401" };
                mock.OverrideMockQueries.Add(m);
                this.RunAndTestImporter(x, tgt, mock);
                mock.OverrideMockQueries.Remove(m);
                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, mock);
            });
        }

        [TestMethod]
        public void TestAppendByIdExclude()
        {
            var x = MockHelper.GetDefaultODataImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbyidexclude;
            var mock = new MockedODataRequestProvider();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values
                var m = new MockedODataQuery() { Url = "https://services.odata.org/V3/Northwind/Northwind.svc/Orders/$count", Value = "10" };
                mock.OverrideMockQueries.Add(m);
                this.RunAndTestImporter(x, tgt, mock);
                mock.OverrideMockQueries.Remove(m);
                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, mock);
            });
        }

        [TestMethod]
        public void TestUpdate()
        {
            var x = MockHelper.GetDefaultODataImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbydate;
            x.UpdateModeEnum = UpdateModeEnum.updateByModifiedDate;
            var format = ODataQueryAdapter.EdmDateTimeFormat; ;
            var currentDate = DateTime.ParseExact("2019-10-27T10:11:12.000", format, System.Globalization.CultureInfo.InvariantCulture);
            var mock = new MockedODataRequestProvider();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                this.RunAndTestImporter(x, tgt);

                var whereClause = "OrderID IN (11076, 11077)";
                var changeField = "ShipPostalCode";
                var changeText = "1337";
                //SqlHelper.ExecuteSqlCommand($"UPDATE {x.SourceName} SET {changeField} = '{changeText}', {x.UpdateQueryDateFieldName} = '{currentDate.ToString(format)}' WHERE {whereClause}", src);

                var m = new MockedODataQuery() { Url = "https://services.odata.org/V3/Northwind/Northwind.svc/Orders/$count?$filter=RequiredDate gt datetime'1998-06-11T00:00:00.000'", Value = "2" };
                var m2 = new MockedODataQuery() { Url = "https://services.odata.org/V3/Northwind/Northwind.svc/Orders?$skip=0&$top=2&$filter=RequiredDate gt datetime'1998-06-11T00:00:00.000'&$orderby=RequiredDate,OrderID", Value = "<?xml version=\"1.0\" encoding=\"utf-8\"?><feed xml:base=\"https://services.odata.org/V3/Northwind/Northwind.svc/\" xmlns=\"http://www.w3.org/2005/Atom\" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><id>https://services.odata.org/V3/Northwind/Northwind.svc/Orders</id><title type=\"text\">Orders</title><updated>2019-10-27T17:23:59Z</updated><link rel=\"self\" title=\"Orders\" href=\"Orders\" /><entry><id>https://services.odata.org/V3/Northwind/Northwind.svc/Orders(11076)</id><category term=\"NorthwindModel.Order\" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\" /><link rel=\"edit\" title=\"Order\" href=\"Orders(11076)\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Customer\" type=\"application/atom+xml;type=entry\" title=\"Customer\" href=\"Orders(11076)/Customer\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Employee\" type=\"application/atom+xml;type=entry\" title=\"Employee\" href=\"Orders(11076)/Employee\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Order_Details\" type=\"application/atom+xml;type=feed\" title=\"Order_Details\" href=\"Orders(11076)/Order_Details\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Shipper\" type=\"application/atom+xml;type=entry\" title=\"Shipper\" href=\"Orders(11076)/Shipper\" /><title /><updated>2019-10-27T17:11:10Z</updated><author><name /></author><content type=\"application/xml\"><m:properties><d:OrderID m:type=\"Edm.Int32\">11076</d:OrderID><d:CustomerID>BONAP</d:CustomerID><d:EmployeeID m:type=\"Edm.Int32\">4</d:EmployeeID><d:OrderDate m:type=\"Edm.DateTime\">1998-05-06T00:00:00</d:OrderDate><d:RequiredDate m:type=\"Edm.DateTime\">2019-10-27T10:11:12</d:RequiredDate><d:ShippedDate m:type=\"Edm.DateTime\" m:null=\"true\" /><d:ShipVia m:type=\"Edm.Int32\">2</d:ShipVia><d:Freight m:type=\"Edm.Decimal\">38.2800</d:Freight><d:ShipName>Bon app'</d:ShipName><d:ShipAddress>12, rue des Bouchers</d:ShipAddress><d:ShipCity>Marseille</d:ShipCity><d:ShipRegion m:null=\"true\" /><d:ShipPostalCode>1337</d:ShipPostalCode><d:ShipCountry>France</d:ShipCountry></m:properties></content></entry><entry><id>https://services.odata.org/V3/Northwind/Northwind.svc/Orders(11077)</id><category term=\"NorthwindModel.Order\" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\" /><link rel=\"edit\" title=\"Order\" href=\"Orders(11077)\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Customer\" type=\"application/atom+xml;type=entry\" title=\"Customer\" href=\"Orders(11077)/Customer\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Employee\" type=\"application/atom+xml;type=entry\" title=\"Employee\" href=\"Orders(11077)/Employee\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Order_Details\" type=\"application/atom+xml;type=feed\" title=\"Order_Details\" href=\"Orders(11077)/Order_Details\" /><link rel=\"http://schemas.microsoft.com/ado/2007/08/dataservices/related/Shipper\" type=\"application/atom+xml;type=entry\" title=\"Shipper\" href=\"Orders(11077)/Shipper\" /><title /><updated>2019-10-27T17:11:10Z</updated><author><name /></author><content type=\"application/xml\"><m:properties><d:OrderID m:type=\"Edm.Int32\">11077</d:OrderID><d:CustomerID>RATTC</d:CustomerID><d:EmployeeID m:type=\"Edm.Int32\">1</d:EmployeeID><d:OrderDate m:type=\"Edm.DateTime\">1998-05-06T00:00:00</d:OrderDate><d:RequiredDate m:type=\"Edm.DateTime\">2019-10-27T10:11:12</d:RequiredDate><d:ShippedDate m:type=\"Edm.DateTime\" m:null=\"true\" /><d:ShipVia m:type=\"Edm.Int32\">2</d:ShipVia><d:Freight m:type=\"Edm.Decimal\">8.5300</d:Freight><d:ShipName>Rattlesnake Canyon Grocery</d:ShipName><d:ShipAddress>2817 Milton Dr.</d:ShipAddress><d:ShipCity>Albuquerque</d:ShipCity><d:ShipRegion>NM</d:ShipRegion><d:ShipPostalCode>1337</d:ShipPostalCode><d:ShipCountry>USA</d:ShipCountry></m:properties></content></entry></feed>" };

                mock.OverrideMockQueries.Add(m);
                mock.OverrideMockQueries.Add(m2);

                this.RunAndTestImporter(x, tgt, mock);
                // Make test right SqlHelper.ExecuteSqlCommand($"UPDATE {x.Name} SET {changeField} = '{changeText}', {x.UpdateQueryDateFieldName} = '{currentDate.ToString(format)}' WHERE {whereClause}", tgt);
                var vals = SqlHelper.GetFieldValues(x.Name, changeField, whereClause, tgt);
                var dates = SqlHelper.GetFieldValueObjects(x.Name, x.UpdateQueryDateFieldName, whereClause, tgt);
                Assert.AreEqual(vals.Length, 2, "Not all vals were returned (not a test architecture fail)");
                Assert.AreEqual(dates.Length, 2, "Not all dates were returned (not a test architecture fail)");

                Assert.IsTrue(vals.All(o => o == changeText), "Value was not updated in all rows");
                Assert.IsTrue(dates.All(o => ((DateTime)o).ToString(format) == currentDate.ToString(format)), "Modified Date is not correct in all rows");
            });
        }

        [TestMethod]
        public void TestConsistencyFix()
        {
            var x = MockHelper.GetDefaultODataImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbyid;
            var mock = new MockedODataRequestProvider();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values
                var m = new MockedODataQuery() { Url = "https://services.odata.org/V3/Northwind/Northwind.svc/Orders/$count", Value = "401" };
                mock.OverrideMockQueries.Add(m);
                this.RunAndTestImporter(x, tgt, mock);
                mock.OverrideMockQueries.Remove(m);
                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, mock);
                var deleteTgtIds = new int[] { 10248, 10249, 10250, 10252, 10253 }; ;
                SqlHelper.RunDeleteScript(x.Name, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteTgtIds)})");
                this.RunAndTestImporter(x, tgt, mock);

            });
        }
    }
}
