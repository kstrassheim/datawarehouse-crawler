using DatawarehouseCrawler.DataAdapters;
using DatawarehouseCrawler.IntegrationTest.Helper;
using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.Providers.DataSetProviders;
using DatawarehouseCrawler.QueryAdapters;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace DatawarehouseCrawler.IntegrationTest
{
    public abstract class AbstractDataSetImporterTest
    {
        [TestInitialize()]
        public void Initialize()
        {
            SqlHelper.ConnectAndRestoreDatabase("Target");
        }

        protected abstract Dictionary<string, IDataSetProvider> providers { get; }

        private void InitAndTestImportTable(Model.ImportModel x, SqlConnection tgt, Action<DataImporter> options = null)
        {
            var sourceAdapter = new DataSetDataAdapter(providers[x.SourceName]);
            var targetAdapter = new DataAdapters.SqlDataAdapter(new Table(x.Name), tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter, ImportOperationMode.GenerateSchema);
            options?.Invoke(y);
            y.Run();
            var tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
            Assert.AreEqual(tableCreated, true, "table not created");
        }

        private DataImporter RunAndTestImporter(Model.ImportModel x, SqlConnection tgt, Action<DataSetDataAdapter> options = null, bool initDataSource = false)
        {
            var sourceAdapter = new DataSetDataAdapter(providers[x.SourceName]);
            if (initDataSource) { sourceAdapter.ReloadAdapter(); }
            var targetAdapter = new DataAdapters.SqlDataAdapter(new Table(x.Name), tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter);
            y.SetPagesize(1000);
            options?.Invoke(sourceAdapter);
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
                this.InitAndTestImportTable(MockHelper.GetDefaultExcelImportSingleIdTable(), tgt);
            });
        }

        [TestMethod]
        public void TestDataCopy()
        {
            var x = MockHelper.GetDefaultExcelImportSingleIdTable();

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                this.RunAndTestImporter(x, tgt);
                var targetRowCount = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.AreEqual(targetRowCount, 6173, "Target row count does not match the example excel file, maybe some rows went missing");
            });
        }

        [TestMethod]
        public void TestAppendById()
        {
            var x = MockHelper.GetDefaultExcelImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbyid;

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values
                this.RunAndTestImporter(x, tgt, prov => prov.DeleteRows("SalesOrderID>70000"));

                var targetRowCount1 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount1 < 6173, "Target row count does not match the example excel file, maybe some rows went missing");
                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, null, true);

                var targetRowCount2 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount2 == 6173, "Target row count does not match the example excel file, maybe some rows went missing");
            });
        }

        [TestMethod]
        public void TestAppendByDate()
        {
            var x = MockHelper.GetDefaultExcelImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbydate;
            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values

                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, prov => prov.DeleteRows("SalesOrderID>70000"));

                var targetRowCount1 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount1 < 6173, "Target row count does not match the example excel file");

                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, null, true);

                var targetRowCount2 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount2 == 6173, "Target row count does not match the example excel file");
            });
        }

        [TestMethod]
        public void TestAppendByIdExclude()
        {
            var x = MockHelper.GetDefaultExcelImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbyidexclude;

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values

                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, prov => prov.DeleteRows("SalesOrderID>69000"));

                var targetRowCount1 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount1 == 50, "Target row count does not match the example excel file");

                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, null, true);

                var targetRowCount2 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount2 == 6173, "Target row count does not match the example excel file");
            });
        }

        [TestMethod]
        public void TestUpdate()
        {
            var x = MockHelper.GetDefaultExcelImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbydate;
            x.UpdateModeEnum = UpdateModeEnum.updateByModifiedDate;
            var format = DataSetQueryAdapter.DateTimeFormat;
            var currentDate = DateTime.Parse("2019-10-27T10:11:12.000");

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                this.RunAndTestImporter(x, tgt);

                var whereClause = "SalesOrderID = 69083 OR SalesOrderID = 69084";

                var changeField = "Comment";
                var changeText = "1337";
                Action<DataRow> change = r =>
                {
                    r[changeField] = changeText;
                    r[x.UpdateQueryDateFieldName] = currentDate;
                };

                // apply change to dataset
                this.RunAndTestImporter(x, tgt, d => d.RunUpdate(change, whereClause));

                var vals = SqlHelper.GetFieldValues(x.Name, changeField, whereClause, tgt);
                var dates = SqlHelper.GetFieldValueObjects(x.Name, x.UpdateQueryDateFieldName, whereClause, tgt);
                Assert.AreEqual(2, vals.Length, "Not all vals were returned (not a test architecture fail)");
                Assert.AreEqual(2, dates.Length, "Not all dates were returned (not a test architecture fail)");

                Assert.IsTrue(vals.All(o => o == changeText), "Value was not updated in all rows");
                Assert.IsTrue(dates.All(o => ((DateTime)o).ToString(format) == currentDate.ToString(format)), "Modified Date is not correct in all rows");
            });
        }

        [TestMethod]
        public void TestConsistencyFix()
        {
            var x = MockHelper.GetDefaultExcelImportSingleIdTable();
            x.DataSyncTypeEnum = Model.DataSyncTypeEnum.appendbyid;

            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                this.InitAndTestImportTable(x, tgt);
                // run once mocked api with reduced values
                this.RunAndTestImporter(x, tgt, prov => prov.DeleteRows("SalesOrderID>70000"));

                var targetRowCount1 = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.IsTrue(targetRowCount1 < 6173, "Target row count does not match the example excel file, maybe some rows went missing");
                // run second mocked api with extended values
                this.RunAndTestImporter(x, tgt, null, true);

                var deleteTgtIds = new int[] { 75123, 75122, 75121, 75028, 75027, 75026, 75025, 75024, 75023, 74919, 74918, 68953, 68952, 68951 };

                SqlHelper.RunDeleteScript(x.Name, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteTgtIds)})");
                this.RunAndTestImporter(x, tgt, null, true);
            });

        }
    }
}
