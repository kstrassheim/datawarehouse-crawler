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
    public class SqlConsistencyFixTest
    {
        [TestInitialize()]
        public void Initialize()
        {
            SqlHelper.ConnectAndRestoreDatabase("Target");
            SqlHelper.ConnectAndRestoreDatabase("src");
        }

        public virtual DataAdapters.SqlDataAdapter GetTargetDataAdapter(Model.ImportModel x, SqlConnection con, bool forceOldSqlVersion = false)
        {
            return new DataAdapters.SqlDataAdapter(new Table(x.Name), con) { AddIgnorePrimaryKeyErrorsInOnPremMode = false, ParameterLimit = 6, ForceOldSqlVersion = forceOldSqlVersion };
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

        private DataImporter RunAndTestImporter(Model.ImportModel x, SqlConnection src, SqlConnection tgt, Action<DataImporter> options = null, bool forceOldSqlVersionSrc = false, bool forceOldSqlVersionTgt = false)
        {
            var sourceRowCount = SqlHelper.GetRowsCount(x.SourceName, src);
            var sourceAdapter = new DataAdapters.SqlDataAdapter(new Table(x.SourceName), src) { ForceOldSqlVersion = forceOldSqlVersionSrc };
            var targetAdapter = this.GetTargetDataAdapter(x, tgt, forceOldSqlVersionTgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter);
            if (options != null) options(y);
            y.Run();
            var targetRowCount = SqlHelper.GetRowsCount(x.Name, tgt);
            Assert.AreEqual(sourceRowCount, targetRowCount, "target row count differs from source");
            return y;
        }

        private void CheckConsistency(Model.ImportModel x, SqlConnection src, SqlConnection tgt, Action<DataImporter> options = null, bool forceOldSqlVersion = false)
        {
            var sourceRowCount = SqlHelper.GetRowsCount(x.SourceName, src);
            var sourceAdapter = new DataAdapters.SqlDataAdapter(new Table(x.SourceName), src) { ForceOldSqlVersion = forceOldSqlVersion };
            var targetAdapter = this.GetTargetDataAdapter(x, tgt);
            var y = new DataImporter(x, sourceAdapter, targetAdapter) { OperationMode = ImportOperationMode.AdvancedConsistencyCheck };
            if (options != null) options(y);
            y.Run();
            var targetRowCount = SqlHelper.GetRowsCount(x.Name, tgt);
            Assert.AreEqual(sourceRowCount, targetRowCount, "target row count differs from source");
            Assert.AreEqual(ConsistencyEnum.OK, y.CountStatus.AdvancedConsistent, "target row count differs from source");
        }

        #endregion abstract

        #region fix ids test

        [TestMethod]
        public void TestFixSingleIdOnlyInsert()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);

                var deleteTgtIds = new int[] { 75123, 75122, 75121, 75028, 75027, 75026, 75025, 75024, 75023, 74919, 74918, 68953, 68952, 68951 };

                SqlHelper.RunDeleteScript(x.Name, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteTgtIds)})");
                this.RunAndTestImporter(x, src, tgt, di=>di.SetConsistencyFixPagesize(100));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixSingleIdOnlyDelete()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var deleteSrcIds = new int[] { 75123, 75122, 75121, 74917, 74918, 74919, 74839, 74836, 68973, 68953, 68952, 68951 };
                SqlHelper.RunDeleteScript(x.SourceName, src, $"{x.IdFieldName} IN ({string.Join(',', deleteSrcIds)})");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixSingleIdBoth()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var deleteSrcIds = new int[] { 74917, 74918, 74919, 74839, 74836, 68973 };
                var deleteTgtIds = new int[] { 75123, 75122, 75121, 75028, 75027, 75026, 75025, 75024, 75023, 74919, 74918, 68953, 68952, 68951 };
                SqlHelper.RunDeleteScript(x.Name, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteTgtIds)})");
                SqlHelper.RunDeleteScript(x.SourceName, src, $"{x.IdFieldName} IN ({string.Join(',', deleteSrcIds)})");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100));
                this.CheckConsistency(x, src, tgt);
            });
        }

        #endregion fix ids test

        #region fix double ids test

        [TestMethod]
        public void TestFixDoubleIdOnlyInsert()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            var idFieldNames = x.IdFieldName.Split(',');
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var deleteTgtIds = new [] { new[] { 75123, 121317 }, new[] { 75123, 121316 }, new[] { 75123, 121315 }, 
                    new[] { 75028, 121088 }, new[] { 75027, 121085 },new[] { 75026, 121082 },new[] { 75025, 121081 },new[] { 75024, 121079 },new[] { 75023, 121077 },new[] { 74919, 121061 },new[] { 75018, 121059 },
                    new[] { 69659, 105262 }, new[] { 69659, 105261 }, new[]{ 69659, 105260 } };
                SqlHelper.RunDeleteScript(x.Name, tgt, $"{string.Join(" OR ", deleteTgtIds.Select(o=> $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixDoubleIdOnlyDelete()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            var idFieldNames = x.IdFieldName.Split(',');
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var deleteSrcIds = new[] { new[] { 75123, 121317 }, new[] { 75123, 121316 }, new[] { 75123, 121315 },
                    new[] { 74978, 120965 }, new[] { 74971, 120951 },new[] { 74970, 120950 },new[] { 74970, 120949 },new[] { 74844, 120665 },new[] { 74836, 120649 },new[] { 74836, 120648 },new[] { 74831, 120637 },
                    new[] { 69659, 105262 }, new[] { 69659, 105261 }, new[]{ 69659, 105260 } };
                SqlHelper.RunDeleteScript(x.SourceName, src, $"{string.Join(" OR ", deleteSrcIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixDoubleIdBoth()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            var idFieldNames = x.IdFieldName.Split(',');
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);
                var deleteTgtIds = new[] { new[] { 75123, 121317 }, new[] { 75123, 121316 }, new[] { 75123, 121315 },
                    new[] { 75028, 121088 }, new[] { 75027, 121085 },new[] { 75026, 121082 },new[] { 75025, 121081 },new[] { 75024, 121079 },new[] { 75023, 121077 },new[] { 74919, 121061 },new[] { 75018, 121059 },
                    new[] { 69659, 105262 }, new[] { 69659, 105261 }, new[]{ 69659, 105260 } };
                var deleteSrcIds = new[] { new[] { 74978, 120965 }, new[] { 74971, 120951 }, new[] { 74970, 120950 }, new[] { 74970, 120949 }, new[] { 74844, 120665 }, new[] { 74836, 120649 }, new[] { 74836, 120648 }, new[] { 74831, 120637 }};

                SqlHelper.RunDeleteScript(x.Name, tgt, $"{string.Join(" OR ", deleteTgtIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                SqlHelper.RunDeleteScript(x.SourceName, src, $"{string.Join(" OR ", deleteSrcIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixDoubleIdBothOldSqlVersionSource()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            var idFieldNames = x.IdFieldName.Split(',');
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt, null, true);
                var deleteTgtIds = new[] { new[] { 75123, 121317 }, new[] { 75123, 121316 }, new[] { 75123, 121315 },
                    new[] { 75028, 121088 }, new[] { 75027, 121085 },new[] { 75026, 121082 },new[] { 75025, 121081 },new[] { 75024, 121079 },new[] { 75023, 121077 },new[] { 74919, 121061 },new[] { 75018, 121059 },
                    new[] { 69659, 105262 }, new[] { 69659, 105261 }, new[]{ 69659, 105260 } };
                var deleteSrcIds = new[] { new[] { 74978, 120965 }, new[] { 74971, 120951 }, new[] { 74970, 120950 }, new[] { 74970, 120949 }, new[] { 74844, 120665 }, new[] { 74836, 120649 }, new[] { 74836, 120648 }, new[] { 74831, 120637 } };

                SqlHelper.RunDeleteScript(x.Name, tgt, $"{string.Join(" OR ", deleteTgtIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                SqlHelper.RunDeleteScript(x.SourceName, src, $"{string.Join(" OR ", deleteSrcIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100), true);
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixDoubleIdBothOldSqlVersionSourceAndTarget()
        {
            var x = MockHelper.GetDefaultSqlImportDoubleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbydate);
            var idFieldNames = x.IdFieldName.Split(',');
            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt, null, true, true);
                var deleteTgtIds = new[] { new[] { 75123, 121317 }, new[] { 75123, 121316 }, new[] { 75123, 121315 },
                    new[] { 75028, 121088 }, new[] { 75027, 121085 },new[] { 75026, 121082 },new[] { 75025, 121081 },new[] { 75024, 121079 },new[] { 75023, 121077 },new[] { 74919, 121061 },new[] { 75018, 121059 },
                    new[] { 69659, 105262 }, new[] { 69659, 105261 }, new[]{ 69659, 105260 } };
                var deleteSrcIds = new[] { new[] { 74978, 120965 }, new[] { 74971, 120951 }, new[] { 74970, 120950 }, new[] { 74970, 120949 }, new[] { 74844, 120665 }, new[] { 74836, 120649 }, new[] { 74836, 120648 }, new[] { 74831, 120637 } };

                SqlHelper.RunDeleteScript(x.Name, tgt, $"{string.Join(" OR ", deleteTgtIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                SqlHelper.RunDeleteScript(x.SourceName, src, $"{string.Join(" OR ", deleteSrcIds.Select(o => $"{idFieldNames.First()} = {o.First()} AND {idFieldNames.Last()} = {o.Last()}"))}");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(100), true, true);
                this.CheckConsistency(x, src, tgt);
            });
        }

        #endregion fix ids test

        [TestMethod]
        public void TestFixSingleIdOnlyInsertFullPageSkip()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);

                var deleteTgtIds = new int[] { 68962, 68960, 68959, 68958, 68957, 68956, 68955, 68954, 68953, 68952, 68951 };

                SqlHelper.RunDeleteScript(x.Name, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteTgtIds)})");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(5));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixSingleIdOnlyDeleteFullPageSkip()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);

                var deleteSrcIds = new int[] { 68962, 68960, 68959, 68958, 68957, 68956, 68955, 68954, 68953, 68952, 68951 };

                SqlHelper.RunDeleteScript(x.SourceName, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteSrcIds)})");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(5));
                this.CheckConsistency(x, src, tgt);
            });
        }

        [TestMethod]
        public void TestFixSingleIdBothFullPageSkip()
        {
            var x = MockHelper.GetDefaultSqlImportSingleIdTable((o) => o.DataSyncTypeEnum = DataSyncTypeEnum.appendbyid);

            SqlHelper.RunConnectionBlock((src, tgt) =>
            {
                this.InitAndTestImportTable(x, src, tgt);
                this.RunAndTestImporter(x, src, tgt);

                var deleteSrcIds = new int[] { 68974, 68972, 68971, 68970, 68969, 68968, 68967, 68966, 68965, 68964, 68961 };
                var deleteTgtIds = new int[] { 68962, 68960, 68959, 68958, 68957, 68956, 68955, 68954, 68953, 68952, 68951 };

                SqlHelper.RunDeleteScript(x.SourceName, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteSrcIds)})");
                SqlHelper.RunDeleteScript(x.Name, tgt, $"{x.IdFieldName} IN ({string.Join(',', deleteTgtIds)})");
                this.RunAndTestImporter(x, src, tgt, di => di.SetConsistencyFixPagesize(5));
                this.CheckConsistency(x, src, tgt);
            });
        }
    }
}