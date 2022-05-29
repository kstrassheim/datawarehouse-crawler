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
using DatawarehouseCrawler.IntegrationTest.Helper;

namespace DatawarehouseCrawler.IntegrationTest
{
    [TestClass]
    public class StatusLoggerTest
    {
        public static bool IsAzureDwh = false;

        [TestInitialize()]
        public void Initialize() {
            SqlHelper.ConnectAndRestoreDatabase("Target");
        }

        [TestMethod]
        public void TestLogInit()
        {
            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                var isl = new ImportStatusLogger(IsAzureDwh, tgt);
                var tableCreated = SqlHelper.IsTableExisting(ImportStatusLogger.LOGTABLENAME, tgt);
                var table2Created = SqlHelper.IsTableExisting(ImportStatusLogger.LOGTABLELOGNAME, tgt);
                Assert.AreEqual(tableCreated, true, "table not created");
                Assert.AreEqual(table2Created, true, "table log not created");
            });
        }

        [TestMethod]
        public void TestLogProgress()
        {
            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                DateTime timestamp = DateTime.Now.AddMilliseconds(-1);
                var isl = new ImportStatusLogger(IsAzureDwh, tgt);
                var o = new ImportStatus() { Name = "Test1", Connection = "tar", Progress = 50, ProgressedRows = 2 };
                isl.UpdateProgress(o);
                var q = isl.GetLog(o.Name, o.Connection);
                Assert.IsNotNull(q, "log does not exists");
                Assert.AreEqual(q.ProgressStatusEnum, ProgressStatusEnum.Idle, "progress is not correct");
                Assert.AreEqual(q.Progress, 50, "progress status is not correct");
                Assert.AreEqual(q.ProgressedRows, 2, "progressed rows are not correct");
                Assert.IsNotNull(q.ProgressModified, "progress modified date was not set");
                //Assert.IsTrue(q.ProgressModified >= timestamp, "date was not updated");
                timestamp = DateTime.Now.AddMilliseconds(-1);
                // update again
                o.Progress = 99;
                o.ProgressedRows = 4;
                isl.UpdateProgress(o);
                q = isl.GetLog(o.Name, o.Connection);
                Assert.IsNotNull(q, "log does not exists");
                Assert.AreEqual(q.ProgressStatusEnum, ProgressStatusEnum.Idle, "progress status is not correct");
                Assert.AreEqual(q.Progress, 99, "progress is not correct");
                Assert.AreEqual(q.ProgressedRows, 4, "progressed rows are not correct");
                Assert.IsNotNull(q.ProgressModified, "progress modified date was not set");
                //Assert.IsTrue(q.ProgressModified >= timestamp, "date was not updated");
            });
        }

        [TestMethod]
        public void TestStatusLog()
        {
            SqlHelper.RunTargetConnectionBlock((tgt) =>
            {
                var timestamp = DateTime.Now.AddMilliseconds(-1);
                var isl = new ImportStatusLogger(IsAzureDwh, tgt);
                var q = new ImportStatus() { Name = "Test1", Connection = "tar", Progress = 99, ProgressedRows = 10 };
                // OK test
                isl.UpdateProgress(q);
                q = isl.GetLog(q.Name, q.Connection);
                Assert.IsNotNull(q, "log does not exists");
                Assert.AreEqual(q.ProgressStatusEnum, ProgressStatusEnum.Idle, "progress is not correct");
                Assert.AreEqual(q.Progress, 99, "progress is not correct");
                Assert.AreEqual(q.ProgressedRows, 10, "progressed rows are not correct");
                Assert.IsNotNull(q.ProgressModified, "progress modified date was not set");
                //Assert.IsTrue(q.ProgressModified > timestamp, "date was not updated");

                // status update OK 
                var timestamp2 = DateTime.Now.AddMilliseconds(-1);
                var duration = (timestamp2 - timestamp).Ticks;
                var duration2 = (timestamp2 - timestamp).Ticks - 1000;
                var duration3 = (timestamp2 - timestamp).Ticks - 2000;
                int rowsCount = 12;
                q.StatusEnum = StatusEnum.OK; q.KeyIntegrityStatusEnum = KeyIntegrityStatusEnum.OK; q.SourceCount = rowsCount; q.TargetCount = rowsCount; q.RowsInserted = rowsCount; q.RowsUpdated = 1; q.ProgressedRows = rowsCount; q.Duration = duration;
                q.ConsitencyFixStatus = 1;
                q.ConsitencyFastFixStatus = 111;
                q.CorrectionRowsInserted = 11;
                q.CorrectionRowsDeleted = 12;
                q.TotalRowsInserted = 13;
                q.TotalRowsDifference = 14;
                q.ConsistencyFixDuration = duration2;
                q.ConsistencyFastFixDuration = duration3;

                isl.UpdateStatus(q);
                q = isl.GetLog(q.Name, q.Connection);
                var ct1 = SqlHelper.GetRowsCount(ImportStatusLogger.LOGTABLELOGSQLNAME, tgt);
                Assert.AreEqual(1, ct1, "Status Log Entry was not written");

                Assert.IsNotNull(q, "log does not exists");
                // test progress
                Assert.AreEqual(q.ProgressStatusEnum, ProgressStatusEnum.None, "progress status was not reset from idle");
                Assert.AreEqual(q.Progress, 100, "progress was not set to complete");
                Assert.AreEqual(q.ProgressedRows, rowsCount, "progressed rows are not correct");
                //Assert.IsTrue(q.ProgressModified > timestamp2, "progress date was not updated");

                // test status updated
                Assert.AreEqual(q.StatusEnum, StatusEnum.OK, "status is not set right");
                Assert.AreEqual(q.KeyIntegrityStatusEnum, KeyIntegrityStatusEnum.OK, "key integrity status is not set right");
                Assert.IsNotNull(q.Modified, "progress modified date was not set");
                //Assert.IsTrue(q.Modified > timestamp2, "modified date was not updated");
                Assert.AreEqual(q.Duration, duration, "duration was not set right");
                Assert.AreEqual(q.ConsistencyFixDuration, duration2, "ConsistencyFixDuration was not set right");
                Assert.AreEqual(q.ConsistencyFastFixDuration, duration3, "ConsistencyFastFixDuration was not set right");
                Assert.AreEqual(q.RowsInserted, rowsCount, "inserted rows are not correct");
                Assert.AreEqual(q.RowsUpdated, 1, "updated rows are not correct");
                Assert.AreEqual(q.SourceCount, rowsCount, "source rows are not correct");
                Assert.AreEqual(q.TargetCount, rowsCount, "target rows are not correct");

                Assert.AreEqual(q.ConsitencyFixStatus, 1, "ConsitencyFixStatus was not set right");
                Assert.AreEqual(q.ConsitencyFastFixStatus, 111, "ConsitencyFixStatus was not set right");
                Assert.AreEqual(q.CorrectionRowsInserted, 11, "CorrectionRowsInserted was not set right");
                Assert.AreEqual(q.CorrectionRowsDeleted, 12, "CorrectionRowsDeleted was not set right");
                Assert.AreEqual(q.TotalRowsInserted, 13, "TotalRowsInserted was not set right");
                Assert.AreEqual(q.TotalRowsDifference, 14, "TotalRowsDifference was not set right");

                Assert.IsNull(q.ErrorMessage, "Error Message should be null");
                // save last data
                var last = new
                {
                    lastStatus = q.Status,
                    lastKeyIntegrityStatus = q.KeyIntegrityStatus,
                    lastModified = q.Modified,
                    lastSourceCount = q.SourceCount,
                    lastTargetCount = q.TargetCount,
                    lastRowsUpdated = q.RowsUpdated,
                    lastRowsInserted = q.RowsInserted,
                    lastErrorMessage = q.ErrorMessage,
                    lastDuration = q.Duration,
                    lastConsistencyFixDuration = q.ConsistencyFixDuration,
                    lastConsistencyFastFixDuration = q.ConsistencyFastFixDuration,
                    lastConsitencyFixStatus = q.ConsitencyFixStatus,
                    lastConsitencyFastFixStatus = q.ConsitencyFastFixStatus,
                    lastCorrectionRowsInserted = q.CorrectionRowsInserted,
                    lastCorrectionRowsDeleted = q.CorrectionRowsDeleted,
                    lastTotalRowsInserted = q.TotalRowsInserted,
                    lastTotalRowsDifference = q.TotalRowsDifference
                };

                // status update Error
                var timestamp3 = DateTime.Now.AddMilliseconds(-1);
                rowsCount = 20;
                var rowsInsert = 0;
                var rowsUpdate = 0;
                duration = (timestamp3 - timestamp2).Ticks;
                duration2 = (timestamp3 - timestamp2).Ticks - 100;
                duration3 = (timestamp3 - timestamp2).Ticks - 200;
                q.StatusEnum = StatusEnum.Error; q.KeyIntegrityStatusEnum = KeyIntegrityStatusEnum.Error; q.SourceCount = rowsCount; q.TargetCount = rowsCount - 1; q.RowsInserted = rowsInsert; q.RowsUpdated = rowsUpdate; q.ProgressedRows = rowsInsert; q.Duration = duration; q.ErrorMessage = "My Error Message";
                q.ConsitencyFixStatus = 2;
                q.ConsitencyFastFixStatus = 222;
                q.CorrectionRowsInserted = 21;
                q.CorrectionRowsDeleted = 22;
                q.TotalRowsInserted = 23;
                q.TotalRowsDifference = 24;
                q.ConsistencyFixDuration = duration2;
                q.ConsistencyFastFixDuration = duration3;
                isl.UpdateStatus(q);
                q = isl.GetLog(q.Name, q.Connection);
                var ct2 = SqlHelper.GetRowsCount(ImportStatusLogger.LOGTABLELOGSQLNAME, tgt);
                Assert.AreEqual(2, ct2, "Status Log Entry was not written");

                // test progress
                Assert.AreEqual(q.ProgressStatusEnum, ProgressStatusEnum.None, "progress status was not reset from idle");
                Assert.AreEqual(q.Progress, 100, "progress was not set to complete");
                Assert.AreEqual(q.ProgressedRows, rowsInsert, "progressed rows are not correct");
                //Assert.IsTrue(q.ProgressModified > timestamp3, "progress date was not updated");

                // test status updated
                Assert.AreEqual(q.StatusEnum, StatusEnum.Error, "status is not set right");
                Assert.AreEqual(q.KeyIntegrityStatusEnum, KeyIntegrityStatusEnum.Error, "key integrity status is not set right");
                Assert.IsNotNull(q.Modified, "progress modified date was not set");
                //Assert.IsTrue(q.Modified > timestamp3, "modified date was not updated");
                Assert.AreEqual(q.Duration, duration, "duration was not set right");
                Assert.AreEqual(q.ConsistencyFixDuration, duration2, "ConsistencyFixDuration was not set right");
                Assert.AreEqual(q.ConsistencyFastFixDuration, duration3, "ConsitencyFastFixStatus was not set right");
                Assert.AreEqual(q.RowsInserted, rowsInsert, "inserted rows are not correct");
                Assert.AreEqual(q.RowsUpdated, rowsUpdate, "updated rows are not correct");
                Assert.AreEqual(q.SourceCount, rowsCount, "source rows are not correct");
                Assert.AreEqual(q.TargetCount, rowsCount - 1, "target rows are not correct");

                Assert.AreEqual(q.ConsitencyFixStatus, 2, "ConsitencyFixStatus was not set right");
                Assert.AreEqual(q.ConsitencyFastFixStatus, 222, "ConsitencyFixStatus was not set right");
                Assert.AreEqual(q.CorrectionRowsInserted, 21, "CorrectionRowsInserted was not set right");
                Assert.AreEqual(q.CorrectionRowsDeleted, 22, "CorrectionRowsDeleted was not set right");
                Assert.AreEqual(q.TotalRowsInserted, 23, "TotalRowsInserted was not set right");
                Assert.AreEqual(q.TotalRowsDifference, 24, "TotalRowsDifference was not set right");

                // test last data
                Assert.AreEqual(last.lastStatus, q.LastStatus, "laststatus data is not right");
                Assert.AreEqual(last.lastKeyIntegrityStatus, q.LastKeyIntegrityStatus, "last key integrity status data is not right");
                Assert.AreEqual(last.lastModified, q.LastModified, "lastmodified data is not right");
                Assert.AreEqual(last.lastSourceCount, q.LastSourceCount, "lastsourcecount data is not right");
                Assert.AreEqual(last.lastTargetCount, q.LastTargetCount, "lasttargetcount data is not right");
                Assert.AreEqual(last.lastRowsInserted, q.LastRowsInserted, "lastrowsinserted data is not right");
                Assert.AreEqual(last.lastRowsUpdated, q.LastRowsUpdated, "lastrowsupdated data is not right");
                Assert.AreEqual(last.lastDuration, q.LastDuration, "lastduration data is not right");
                Assert.AreEqual(last.lastConsistencyFixDuration, q.LastConsistencyFixDuration, "lastConsistencyFixDuration data is not right");
                Assert.AreEqual(last.lastConsistencyFastFixDuration, q.LastConsistencyFastFixDuration, "lastConsistencyFastFixDuration data is not right");
                Assert.AreEqual(last.lastErrorMessage, q.LastErrorMessage, "lasterrormessage data is not right");

                Assert.AreEqual(last.lastConsitencyFixStatus, q.LastConsitencyFixStatus, "LastConsitencyFixStatus data is not right");
                Assert.AreEqual(last.lastConsitencyFastFixStatus, q.LastConsitencyFastFixStatus, "LastConsitencyFastFixStatus data is not right");
                Assert.AreEqual(last.lastCorrectionRowsInserted, q.LastCorrectionRowsInserted, "LastCorrectionRowsInserted data is not right");
                Assert.AreEqual(last.lastCorrectionRowsDeleted, q.LastCorrectionRowsDeleted, "LastCorrectionRowsDeleted data is not right");
                Assert.AreEqual(last.lastTotalRowsInserted, q.LastTotalRowsInserted, "LastTotalRowsInserted data is not right");
                Assert.AreEqual(last.lastTotalRowsDifference, q.LastTotalRowsDifference, "LastTotalRowsDifference data is not right");


                // save last data
                last = new
                {
                    lastStatus = q.Status,
                    lastKeyIntegrityStatus = q.KeyIntegrityStatus,
                    lastModified = q.Modified,
                    lastSourceCount = q.SourceCount,
                    lastTargetCount = q.TargetCount,
                    lastRowsUpdated = q.RowsUpdated,
                    lastRowsInserted = q.RowsInserted,
                    lastErrorMessage = q.ErrorMessage,
                    lastDuration = q.Duration,
                    lastConsistencyFixDuration = q.ConsistencyFixDuration,
                    lastConsistencyFastFixDuration = q.ConsistencyFastFixDuration,
                    lastConsitencyFixStatus = q.ConsitencyFixStatus,
                    lastConsitencyFastFixStatus = q.ConsitencyFastFixStatus,
                    lastCorrectionRowsInserted = q.CorrectionRowsInserted,
                    lastCorrectionRowsDeleted = q.CorrectionRowsDeleted,
                    lastTotalRowsInserted = q.TotalRowsInserted,
                    lastTotalRowsDifference = q.TotalRowsDifference
                };
                // status update Warning
                var timestamp4 = DateTime.Now.AddMilliseconds(-1);
                rowsCount = 20;
                rowsInsert = 7;
                rowsUpdate = 1;
                duration = (timestamp4 - timestamp3).Ticks;
                duration2 = (timestamp4 - timestamp3).Ticks - 100;
                duration3 = (timestamp4 - timestamp3).Ticks - 300;
                q.StatusEnum = StatusEnum.Warning; q.KeyIntegrityStatusEnum = KeyIntegrityStatusEnum.DoubleKeys; q.SourceCount = 20; q.TargetCount = rowsCount - 1; q.RowsInserted = rowsInsert; q.RowsUpdated = rowsUpdate; q.ProgressedRows = rowsInsert + rowsUpdate; q.Duration = duration;
                q.ConsitencyFixStatus = 3;
                q.ConsitencyFastFixStatus = 333;
                q.CorrectionRowsInserted = 31;
                q.CorrectionRowsDeleted = 32;
                q.TotalRowsInserted = 33;
                q.TotalRowsDifference = 34;
                q.ConsistencyFixDuration = duration2;
                q.ConsistencyFastFixDuration = duration3;
                isl.UpdateStatus(q);
                q = isl.GetLog(q.Name, q.Connection);
                var ct3 = SqlHelper.GetRowsCount(ImportStatusLogger.LOGTABLELOGSQLNAME, tgt);
                Assert.AreEqual(3, ct3, "Status Log Entry was not written");
                // test progress
                Assert.AreEqual(q.ProgressStatusEnum, ProgressStatusEnum.None, "progress status was not reset from idle");
                Assert.AreEqual(q.Progress, 100, "progress was not set to complete");
                Assert.AreEqual(q.ProgressedRows, rowsInsert + rowsUpdate, "progressed rows are not correct");
                //Assert.IsTrue(q.ProgressModified > timestamp4, "progress date was not updated");

                // test status updated
                Assert.AreEqual(q.StatusEnum, StatusEnum.Warning, "status is not set right");
                Assert.AreEqual(q.KeyIntegrityStatusEnum, KeyIntegrityStatusEnum.DoubleKeys, "key integrity status is not set right");
                Assert.IsNotNull(q.Modified, "progress modified date was not set");
                //Assert.IsTrue(q.Modified > timestamp4, "modified date was not updated");
                Assert.AreEqual(q.Duration, duration, "duration was not set right");
                Assert.AreEqual(q.ConsistencyFixDuration, duration2, "ConsistencyFixDuration was not set right");
                Assert.AreEqual(q.ConsistencyFastFixDuration, duration3, "ConsistencyFixDuration was not set right");
                Assert.AreEqual(q.RowsInserted, rowsInsert, "inserted rows are not correct");
                Assert.AreEqual(q.RowsUpdated, rowsUpdate, "updated rows are not correct");
                Assert.AreEqual(q.SourceCount, rowsCount, "source rows are not correct");
                Assert.AreEqual(q.TargetCount, rowsCount - 1, "target rows are not correct");
                Assert.IsNull(q.ErrorMessage, "Error Message should be null");

                Assert.AreEqual(q.ConsitencyFixStatus, 3, "ConsitencyFixStatus was not set right");
                Assert.AreEqual(q.ConsitencyFastFixStatus, 333, "ConsitencyFastFixStatus was not set right");
                Assert.AreEqual(q.CorrectionRowsInserted, 31, "CorrectionRowsInserted was not set right");
                Assert.AreEqual(q.CorrectionRowsDeleted, 32, "CorrectionRowsDeleted was not set right");
                Assert.AreEqual(q.TotalRowsInserted, 33, "TotalRowsInserted was not set right");
                Assert.AreEqual(q.TotalRowsDifference, 34, "TotalRowsDifference was not set right");

                // test last data
                Assert.AreEqual(last.lastStatus, q.LastStatus, "laststatus data is not right");
                Assert.AreEqual(last.lastKeyIntegrityStatus, q.LastKeyIntegrityStatus, "last key integrity status data is not right");
                Assert.AreEqual(last.lastModified, q.LastModified, "lastmodified data is not right");
                Assert.AreEqual(last.lastSourceCount, q.LastSourceCount, "lastsourcecount data is not right");
                Assert.AreEqual(last.lastTargetCount, q.LastTargetCount, "lasttargetcount data is not right");
                Assert.AreEqual(last.lastRowsInserted, q.LastRowsInserted, "lastrowsinserted data is not right");
                Assert.AreEqual(last.lastRowsUpdated, q.LastRowsUpdated, "lastrowsupdated data is not right");
                Assert.AreEqual(last.lastDuration, q.LastDuration, "lastduration data is not right");

                Assert.AreEqual(last.lastConsistencyFixDuration, q.LastConsistencyFixDuration, "lastConsistencyFixDuration data is not right");
                Assert.AreEqual(last.lastConsistencyFastFixDuration, q.LastConsistencyFastFixDuration, "lastConsistencyFastFixDuration data is not right");
                Assert.AreEqual(last.lastErrorMessage, q.LastErrorMessage, "lasterrormessage data is not right");

                Assert.AreEqual(last.lastConsitencyFixStatus, q.LastConsitencyFixStatus, "LastConsitencyFixStatus data is not right");
                Assert.AreEqual(last.lastConsitencyFastFixStatus, q.LastConsitencyFastFixStatus, "LastConsitencyFastFixStatus data is not right");
                Assert.AreEqual(last.lastCorrectionRowsInserted, q.LastCorrectionRowsInserted, "LastCorrectionRowsInserted data is not right");
                Assert.AreEqual(last.lastCorrectionRowsDeleted, q.LastCorrectionRowsDeleted, "LastCorrectionRowsDeleted data is not right");
                Assert.AreEqual(last.lastTotalRowsInserted, q.LastTotalRowsInserted, "LastTotalRowsInserted data is not right");
                Assert.AreEqual(last.lastTotalRowsDifference, q.LastTotalRowsDifference, "LastTotalRowsDifference data is not right");
            });
        }
    }
}
