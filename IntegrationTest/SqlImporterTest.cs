using System;
using System.IO;
using System.Configuration;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text.RegularExpressions;
using DatawarehouseCrawler.Model;
using LOG = DatawarehouseCrawler.Model.Log;
using System.Threading.Tasks;
using System.Threading;
using DatawarehouseCrawler.IntegrationTest.Helper;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler;
using DatawarehouseCrawler.QueryAdapters;

namespace DatawarehouseCrawler.IntegrationTest
{
    [TestClass]
    public class SqlImporterTest
    {
        [TestInitialize()]
        public void Initialize()
        {
            SqlHelper.ConnectAndRestoreDatabase("Target");
            SqlHelper.ConnectAndRestoreDatabase("src");
        }

        public virtual DataAdapters.SqlDataAdapter GetTargetDataAdapter(Model.ImportModel x, SqlConnection con)
        {
            return new DataAdapters.SqlDataAdapter(new Table(x.Name), con) { AddIgnorePrimaryKeyErrorsInOnPremMode = false }; 
        }

        public virtual void ApplyAfterSchemaTest(DataImporter importer, SqlConnection src, SqlConnection tgt) {
            // check primary key copy
            var srcPkeys = SqlHelper.getPrimaryKeys(importer.Import.SourceName, src);
            var targetPkeys = SqlHelper.getPrimaryKeys(importer.Import.Name, tgt);

            Assert.AreEqual(srcPkeys.Length, targetPkeys.Length, "The length of the primary keys differs");
            CollectionAssert.AreEqual(srcPkeys, targetPkeys, "the primary keys were not copied correct");
        }

        public virtual void ApplyAfterDataTest(DataImporter importer) {  }

        [TestMethod]
        public void TestSchemaCopy()
        {
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                var x = MockHelper.GetDefaultSqlImportDoubleIdTable();
                var sourceAdapter = new DataAdapters.SqlDataAdapter(new Table(x.SourceName), src);
                var targetAdapter = this.GetTargetDataAdapter(x, tgt);

                var p = new DataImporter(x, sourceAdapter, targetAdapter, ImportOperationMode.GenerateSchema);
                p.Run();

                var tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
                Assert.AreEqual(tableCreated, true, "table not created");

                p.Force = true;
                p.Run();

                tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
                Assert.AreEqual(tableCreated, true, "table not created");

                this.ApplyAfterSchemaTest(p, src, tgt);
            });
        }

        [TestMethod]
        public void TestQuerySourceDB()
        {
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                var ct = SqlHelper.GetRowsCount("[AdventureWorks2017].[Sales].[SalesOrderDetail]", src);
                Assert.AreEqual(ct, 16058);
            });
        }

        #region abstract

        private void InitAndTestImportTable(Model.ImportModel x, SqlConnection src, SqlConnection tgt)
        {
            var sourceAdapter = new DataAdapters.SqlDataAdapter(new Table(x.SourceName), src);
            var targetAdapter = this.GetTargetDataAdapter(x, tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter, ImportOperationMode.GenerateSchema);
            y.Run();

            var tableCreated = SqlHelper.IsTableExisting(x.Name, tgt);
            Assert.AreEqual(tableCreated, true, "table not created");
            
        }

        private DataImporter RunAndTestImporter(Model.ImportModel x, SqlConnection src, SqlConnection tgt, Action<DataImporter> options = null, bool forceOldSqlVersion = false)
        {
            var sourceRowCount = SqlHelper.GetRowsCount(x.SourceName, src);
            var sourceAdapter = new DataAdapters.SqlDataAdapter(new Table(x.SourceName), src) { ForceOldSqlVersion = forceOldSqlVersion };
            var targetAdapter = this.GetTargetDataAdapter(x, tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter);

            if (options != null) options(y);
            y.Run();
            var targetRowCount = SqlHelper.GetRowsCount(x.Name, tgt);
            Assert.AreEqual(sourceRowCount, targetRowCount, "target row count differs from source");
            this.ApplyAfterDataTest(y);
            return y;
        }

        private void TestAppendAlgorithm(Model.ImportModel x, string deleteSourceWhere, string deleteRestoreScriptName, Action<Model.ImportModel> setItemOptions = null, bool forceOldSqlVersion = false)
        {
            if (setItemOptions != null) setItemOptions(x);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                SqlHelper.RunDeleteScript(x.SourceName, src, deleteSourceWhere);
                this.RunAndTestImporter(x, src, tgt, (y) => y.SetPagesize(100), forceOldSqlVersion);

                // delete source lines
                SqlHelper.ExecuteSqlFile(deleteRestoreScriptName, src);
                var sourceRowCount = SqlHelper.GetRowsCount(x.SourceName, src);
                var targetRowCount = SqlHelper.GetRowsCount(x.Name, tgt);
                Assert.AreNotEqual(sourceRowCount, targetRowCount, "target row count is equal to source, after adding more rows to source");

                // run twice
                this.RunAndTestImporter(x, src, tgt, (y) => y.SetPagesize(100));
                this.RunAndTestImporter(x, src, tgt, (y) => y.SetPagesize(100));
            });
        }

        private void TestExceptionalRun(Model.ImportModel x, Action<Model.ImportModel> modify = null)
        {
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                modify?.Invoke(x);
                var sourceAdapter = new DataAdapters.SqlDataAdapter(new Table(x.SourceName), src);
                var targetAdapter = new DataAdapters.SqlDataAdapter(new Table(x.Name), tgt);
                var im = new DataImporter(x, sourceAdapter, targetAdapter);
                im.Run();
            });
        }

        private void CheckCountTest(Model.ImportModel x, LOG.LogEventType expectedResult, string assertText, Action<SqlConnection, SqlConnection> optional = null)
        {
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                var im = this.RunAndTestImporter(x, src, tgt);
                if (optional != null) optional(src, tgt);

                var result = false;
                im.OnLogMessage += delegate (object o, LOG.LogEventArgs e) {
                    if (e.Message.StartsWith("Count - "))
                    {
                        result = e.Type == expectedResult;
                    }
                };
                im.OperationMode = ImportOperationMode.CheckCount;
                im.Run();

                Assert.IsTrue(result, assertText);
            });
        }

        #endregion abstract

        #region datacopytests

        [TestMethod]
        public void TestDataCopy()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable();

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestPartialDataCopy()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable();

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt, (y) => y.SetPagesize(100)) ;
            });
        }

        //[TestMethod]
        //public void TestPartialDataCopyTwiceIgnoringPKErrors()
        //{
        //    var x = MockHelper.GetDefaultSqlImportDoubleIdTable();
        //    SqlHelper.RunConnectionBlock((src, tgt) =>
        //    {
        //        this.InitAndTestImportTable(x, src, tgt);
        //        this.RunAndTestImporter(x, src, tgt, (y) => y.SetPagesize(100));
        //        this.RunAndTestImporter(x, src, tgt, (y) => y.SetPagesize(100));
        //    });
        //}

        #endregion datacopytests

        #region appendbyidtests

        [TestMethod]
        public void TestAppendById()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);
            this.TestAppendAlgorithm(x, $"{x.IdFieldName} > 75000", "addlines_appendbyid.sql");
        }

        [TestMethod]
        public void TestAppendByIdOldSqlVersion()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);
            this.TestAppendAlgorithm(x, $"{x.IdFieldName} > 75000", "addlines_appendbyid.sql", null, true);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "when idfieldname is empty argument exception should be thrown")]
        public void TestAppendByIdNoIdProvidedException()
        {
            this.TestExceptionalRun(MockHelper.GetDefaultSqlImportDoubleIdTable(), (o) => { o.IdFieldName = string.Empty; o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid; });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "appendbyid should only work with one id")]
        public void TestAppendByIdMultipleIdException()
        {
            this.TestExceptionalRun(MockHelper.GetDefaultSqlImportDoubleIdTable(), (o) => { o.IdFieldName = "rowguid"; o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid; });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "appendbyid should only work with integer types")]
        public void TestAppendByIdOnlyIntegerTypeException()
        {
            this.TestExceptionalRun(MockHelper.GetDefaultSqlImportSingleIdTable(), (o) => { o.IdFieldName = "rowguid"; o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid; });
        }

        #endregion appendbyidtests

        #region appendbyidexcludetests

        [TestMethod]
        public void TestAppendByIdExclude()
        {
            
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyidexclude);
            this.TestAppendAlgorithm(x, $"{x.IdFieldName} > 75000 OR {x.IdFieldName} < 74000", "addlines_appendbyid.sql");
        }

        [TestMethod]
        public void TestAppendByIdExcludeOldSqlVersion()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyidexclude);
            this.TestAppendAlgorithm(x, $"{x.IdFieldName} > 75000 OR {x.IdFieldName} < 74000", "addlines_appendbyid.sql", null,  true);
        }

        [TestMethod]
        public void TestAppendByIdExcludeAdditionalGuidId()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => { o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyidexclude; o.IdFieldName += ",rowguid"; });
            this.TestAppendAlgorithm(x, $"{x.InsertQueryDateFieldName} > '2014-06-29' OR {x.InsertQueryDateFieldName} < '2014-06-22'", "addlines_appendbydate.sql");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "when idfieldname is empty argument exception should be thrown")]
        public void TestAppendByIdExcludeNoIdProvidedException()
        {
            this.TestExceptionalRun(MockHelper.GetDefaultSqlImportDoubleIdTable(), (o) => { o.IdFieldName = string.Empty; o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyidexclude; });
        }

        #endregion appendbyidexcludetests

        #region appendbydatetests

        [TestMethod]
        public void TestAppendByDate()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o)=>o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            this.TestAppendAlgorithm(x, $"{x.InsertQueryDateFieldName} > '2014-06-29' OR {x.InsertQueryDateFieldName} < '2014-03-01'", "addlines_appendbydate.sql");
        }

        [TestMethod]
        public void TestJoinAppendByDate()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdJoinTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            this.TestAppendAlgorithm(x, $"[ModifiedDate] > '2014-06-29' OR [ModifiedDate] < '2014-03-01'", "addlines_appendbydate.sql");
        }

        [TestMethod]
        public void TestJoinAppendByDateOldSqlVersion()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdJoinTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            this.TestAppendAlgorithm(x, $"[ModifiedDate] > '2014-06-29' OR [ModifiedDate] < '2014-03-01'", "addlines_appendbydate.sql", null, true);
        }

        [TestMethod]
        public void TestAppendByDateOldSqlVersion()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            this.TestAppendAlgorithm(x, $"{x.InsertQueryDateFieldName} > '2014-06-29' OR {x.InsertQueryDateFieldName} < '2014-03-01'", "addlines_appendbydate.sql", null, true);
        }

        [TestMethod]
        public void TestAppendByDateWithAdditionalGuidId()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => { o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate; o.IdFieldName += ",rowguid"; });
            this.TestAppendAlgorithm(x, $"{x.InsertQueryDateFieldName} > '2014-06-29' OR {x.InsertQueryDateFieldName} < '2014-03-01'", "addlines_appendbydate.sql");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "when idfieldname is empty then argument exception should be thrown")]
        public void TestAppendByDateNoIdProvidedException()
        {
            this.TestExceptionalRun(MockHelper.GetDefaultSqlImportDoubleIdTable(), (o) => { o.IdFieldName = string.Empty; o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate; });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "when updatedatefieldname is empty then argument exception should be thrown")]
        public void TestAppendByDateNoUpdateDateFieldNameProvidedException()
        {
            this.TestExceptionalRun(MockHelper.GetDefaultSqlImportDoubleIdTable(), (o) => { o.InsertQueryDateFieldName = string.Empty; o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate; });
        }

        #endregion appendbydatetests

        #region update tests

        [TestMethod]
        public void TestUpdate()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            x.UpdateModeEnum = UpdateModeEnum.updateByModifiedDate;
            var format = "yyyy-MM-dd HH:mm:ss";
            var currentDate = DateTime.Now;

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var whereClause = "SalesOrderID IN (75001, 75007)";
                var changeField = "Comment";
                var changeText = "Update Test";
                SqlHelper.ExecuteSqlCommand($"UPDATE {x.SourceName} SET {changeField} = '{changeText}', {x.UpdateQueryDateFieldName} = '{currentDate.ToString(format)}' WHERE {whereClause}", src);
                this.RunAndTestImporter(x, src, tgt);
                // Make test right SqlHelper.ExecuteSqlCommand($"UPDATE {x.Name} SET {changeField} = '{changeText}', {x.UpdateQueryDateFieldName} = '{currentDate.ToString(format)}' WHERE {whereClause}", tgt);
                var vals = SqlHelper.GetFieldValues(x.Name, changeField, whereClause, tgt);
                var dates = SqlHelper.GetFieldValueObjects(x.Name, x.UpdateQueryDateFieldName, whereClause, tgt);
                Assert.AreEqual(vals.Length, 2, "Not all vals were returned (not a test architecture fail)");
                Assert.AreEqual(dates.Length, 2, "Not all dates were returned (not a test architecture fail)");

                Assert.IsTrue(vals.All(o=>o == changeText), "Value was not updated in all rows");
                Assert.IsTrue(dates.All(o => ((DateTime)o).ToString(format) == currentDate.ToString(format)), "Modified Date is not correct in all rows");
            });
        }

        [TestMethod]
        public void TestUpdateAllEntries()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            x.UpdateModeEnum = UpdateModeEnum.updateByModifiedDate;
            var format = "yyyy-MM-dd HH:mm:ss";
            var currentDate = DateTime.Now;

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var changeField = "Comment";
                // set comment to id
                SqlHelper.ExecuteSqlCommand($"UPDATE {x.SourceName} SET {changeField} = SalesOrderID, {x.UpdateQueryDateFieldName} = '{currentDate.ToString(format)}'", src);
                this.RunAndTestImporter(x, src, tgt, im=>im.SetPagesize(1503));
                var ct = SqlHelper.GetRowsCount(x.Name, tgt);
                // Make test right SqlHelper.ExecuteSqlCommand($"UPDATE {x.Name} SET {changeField} = '{changeText}', {x.UpdateQueryDateFieldName} = '{currentDate.ToString(format)}' WHERE {whereClause}", tgt);
                var vals = SqlHelper.GetFieldValueObjects(x.Name, new string[]{"SalesOrderID",changeField,x.UpdateQueryDateFieldName}, string.Empty, tgt);
                Assert.AreEqual(vals.Count(), ct, "Not all object were returned");
                int i = 0; string checkDate = currentDate.ToString(format);
                foreach (var v in vals)
                {
                    int id = (int)v[0];
                    string c = v[1].ToString();
                    string d = ((DateTime)v[2]).ToString(format);
                    Assert.IsTrue(!string.IsNullOrEmpty(c), $"Comment has no value on item {i}");
                    Assert.AreEqual(id.ToString(), c, $"Comment was not changed on item {i}");
                    Assert.AreEqual(checkDate, d, $"Update Date time is wrong on item {i}");
                    i++;
                }
            });
        }

        #endregion update tests

        #region checkcounttests

        [TestMethod]
        public void TestCheckCountModeEqual()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable();
            this.CheckCountTest(x, LOG.LogEventType.Success, "Message count should be success on equal");
        }

        [TestMethod]
        public void TestCheckCountModeDifference()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable();
            this.CheckCountTest(x, LOG.LogEventType.Warning, "Message type should be warning on difference", (src, tgt) => SqlHelper.RunDeleteScript(x.SourceName, src, $"{x.InsertQueryDateFieldName} > '2014-06-29'"));
        }

        #endregion checkcounttests
    }
}
