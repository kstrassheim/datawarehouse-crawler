using DatawarehouseCrawler.Model;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler
{
    public class ImportStatusLogger
    {
        public const string LOGTABLENAME = "ImportStatus";
        public const string LOGTABLESQLNAME = "[dbo].[ImportStatus]";
        public const string LOGTABLELOGNAME = "ImportStatusLog";
        public const string LOGTABLELOGSQLNAME = "[dbo].[ImportStatusLog]";

        public const string LOGTABLEIDFIELDNAME = "[Name]";
        public const string CREATESTATUSTABLESCRIPT = @"CREATE TABLE [dbo].[ImportStatus](
            [Name][nvarchar](50) NOT NULL,
            [Connection] [nvarchar] (20) NOT NULL,
            [Status] [int] NOT NULL,
            [LastStatus] [int] NOT NULL,
            [KeyIntegrityStatus] [int] NOT NULL,
            [LastKeyIntegrityStatus] [int] NOT NULL,
            [ConsitencyFixStatus] [int] NOT NULL,
            [ConsitencyFastFixStatus] [int] NOT NULL,
            [LastConsitencyFastFixStatus] [int] NOT NULL,
            [LastConsitencyFixStatus] [int] NOT NULL,
	        [SourceCount] [int] NOT NULL,
	        [TargetCount] [int] NOT NULL,
	        [LastSourceCount] [int] NOT NULL,
	        [LastTargetCount] [int] NOT NULL,
	        [RowsInserted] [int] NOT NULL,
	        [LastRowsInserted] [int] NOT NULL,
            [RowsUpdated] [int] NOT NULL,
	        [LastRowsUpdated] [int] NOT NULL,
            [CorrectionRowsInserted] [int] NOT NULL,
            [LastCorrectionRowsInserted] [int] NOT NULL,
            [CorrectionRowsDeleted] [int] NOT NULL,
            [LastCorrectionRowsDeleted] [int] NOT NULL,
            [TotalRowsInserted] [int] NOT NULL,
            [LastTotalRowsInserted] [int] NOT NULL,
            [TotalRowsDifference] [int] NOT NULL,
            [LastTotalRowsDifference] [int] NOT NULL,
            [ProgressStatus] [int] NOT NULL,
	        [Progress] [int] NOT NULL,
            [ProgressedRows] [int] NOT NULL,
            [ProgressModified] [datetime] NOT NULL,
	        [Duration] [bigint] NOT NULL,
	        [LastDuration] [bigint] NOT NULL,
            [ConsistencyFixDuration] [bigint] NOT NULL,
            [LastConsistencyFixDuration] [bigint] NOT NULL,
            [ConsistencyFastFixDuration] [bigint] NOT NULL,
            [LastConsistencyFastFixDuration] [bigint] NOT NULL,
	        [Modified] [datetime] NOT NULL,
	        [LastModified] [datetime] NULL,
	        [ApplyFunction] [int] NOT NULL,
	        [ErrorMessage] [nvarchar] (255) NULL,
	        [LastErrorMessage] [nvarchar] (255) NULL
            {0}
            )
         ";

        public const string CREATESTATUSTABLELOGSCRIPT = @"CREATE TABLE [dbo].[ImportStatusLog](
            [Id] [bigint] {0} NOT NULL,
            [Name] [nvarchar](50) NOT NULL,
            [Connection] [nvarchar] (20) NOT NULL,
            [Status] [int] NOT NULL,
            [KeyIntegrityStatus] [int] NOT NULL,
            [ConsitencyFixStatus] [int] NOT NULL,
            [ConsitencyFastFixStatus] [int] NOT NULL,
	        [SourceCount] [int] NOT NULL,
	        [TargetCount] [int] NOT NULL,
	        [RowsInserted] [int] NOT NULL,
            [RowsUpdated] [int] NOT NULL,
            [CorrectionRowsInserted] [int] NOT NULL,
            [CorrectionRowsDeleted] [int] NOT NULL,
            [TotalRowsInserted] [int] NOT NULL,
            [TotalRowsDifference] [int] NOT NULL,
	        [Duration] [bigint] NOT NULL,
            [ConsistencyFixDuration] [bigint] NOT NULL,
            [ConsistencyFastFixDuration] [bigint] NOT NULL,
	        [ErrorMessage] [nvarchar] (255) NULL,
            [Created] [datetime] NOT NULL
            )
         ";

        public const string CREATESTATUSTABLEPKCONSTRANT = "CONSTRAINT[PK_ImportStatus] PRIMARY KEY CLUSTERED  ([Name] ASC )WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]";
        private bool initialized = false;
        private bool IsAzureDwh { get; set; }

        protected SqlConnection Connection { get; private set; }

        public ImportStatusLogger(bool isAzureDwh, SqlConnection connection)
        {
            this.IsAzureDwh = isAzureDwh;
            this.Init(connection);
        }

        public void Init(SqlConnection connection)
        {
            this.initialized = false;
            this.Connection = connection;

            if (this.Connection != null && this.Connection.State != System.Data.ConnectionState.Open) throw new ArgumentException("Connection must be opened");

            this.EnsureLogTableExists();
            this.initialized = true;
        }

        public ImportStatus GetLog(string name, string connectionName)
        {
            if (!this.initialized) throw new Exception("Not initialized");
            if (this.Connection != null && this.Connection.State != System.Data.ConnectionState.Open) throw new Exception("Connection must be opened");
            try
            {
                if (!this.Exists(name)) this.InsertNewRow(name, connectionName);
                var cmd = new SqlCommand($"SELECT * FROM {LOGTABLESQLNAME} WHERE {LOGTABLEIDFIELDNAME} = @Name", this.Connection);
                cmd.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = name;
                var rd = cmd.ExecuteReader();
                ImportStatus o = null;
                if (rd.Read()) { o = this.ConvertDbReaderToObject(rd); rd.Close(); }
                else throw new Exception($"Entry {name} does not exist");
                return o;
            }
            catch
            {
                throw;
            }
        }

        public void ResetApplyFunctionValue(string name)
        {
            if (!this.initialized) throw new Exception("Not initialized");

            try
            {
                var cmd = new SqlCommand($@"UPDATE {LOGTABLESQLNAME} SET ApplyFunction = 0 WHERE {LOGTABLEIDFIELDNAME} = @Name", this.Connection);
                cmd.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = name;
                cmd.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
        }

        public void UpdateStatus(ImportStatus o)
        {
            if (!this.initialized) throw new Exception("Not initialized");
            var now = DateTime.Now;
            try
            {
                // if not exist try insert new
                if (!this.Exists(o)) this.InsertNewRow(o);
                var cmd = new SqlCommand("", this.Connection);

                StringBuilder query = new StringBuilder($@"UPDATE {LOGTABLESQLNAME} SET 
                        LastStatus = Status,
                        LastKeyIntegrityStatus = KeyIntegrityStatus,
                        LastConsitencyFixStatus = ConsitencyFixStatus,
                        LastConsitencyFastFixStatus = ConsitencyFastFixStatus,
                        LastSourceCount = SourceCount,
                        LastTargetCount = TargetCount,
                        LastRowsInserted = RowsInserted,
                        LastRowsUpdated = RowsUpdated,
                        LastDuration = Duration,
                        LastConsistencyFixDuration = ConsistencyFixDuration,
                        LastConsistencyFastFixDuration = ConsistencyFastFixDuration,
                        LastErrorMessage = ErrorMessage,
                        LastModified = Modified,
                        LastCorrectionRowsInserted = CorrectionRowsInserted,
                        LastCorrectionRowsDeleted = CorrectionRowsDeleted,
                        LastTotalRowsInserted = TotalRowsInserted,
                        LastTotalRowsDifference = TotalRowsDifference,
                        ProgressStatus = 0,
                        Progress = 100,
                        ApplyFunction = 0,
                        Status = @Status,
                        KeyIntegrityStatus = @KeyIntegrityStatus,
                        ConsitencyFixStatus = @ConsitencyFixStatus,
                        ConsitencyFastFixStatus = @ConsitencyFastFixStatus,
                        SourceCount = @SourceCount,
                        TargetCount = @TargetCount,
                        RowsInserted = @RowsInserted,
                        RowsUpdated = @RowsUpdated,
                        CorrectionRowsInserted = @CorrectionRowsInserted,
                        CorrectionRowsDeleted = @CorrectionRowsDeleted,
                        TotalRowsInserted = @TotalRowsInserted,
                        TotalRowsDifference = @TotalRowsDifference,
                        ProgressedRows = @ProgressedRows,
                        Duration = @Duration,
                        ConsistencyFixDuration = @ConsistencyFixDuration,
                        ConsistencyFastFixDuration = @ConsistencyFastFixDuration,
                    ");

               

                cmd.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = o.Name;
                cmd.Parameters.Add("@Status", System.Data.SqlDbType.Int).Value = o.Status;
                cmd.Parameters.Add("@KeyIntegrityStatus", System.Data.SqlDbType.Int).Value = o.KeyIntegrityStatus;
                cmd.Parameters.Add("@ConsitencyFixStatus", System.Data.SqlDbType.Int).Value = o.ConsitencyFixStatus;
                cmd.Parameters.Add("@ConsitencyFastFixStatus", System.Data.SqlDbType.Int).Value = o.ConsitencyFastFixStatus;
                cmd.Parameters.Add("@SourceCount", System.Data.SqlDbType.Int).Value = o.SourceCount;
                cmd.Parameters.Add("@TargetCount", System.Data.SqlDbType.Int).Value = o.TargetCount;
                cmd.Parameters.Add("@RowsInserted", System.Data.SqlDbType.Int).Value = o.RowsInserted;
                cmd.Parameters.Add("@RowsUpdated", System.Data.SqlDbType.Int).Value = o.RowsUpdated;
                cmd.Parameters.Add("@ProgressedRows", System.Data.SqlDbType.Int).Value = o.ProgressedRows;
                cmd.Parameters.Add("@CorrectionRowsInserted", System.Data.SqlDbType.Int).Value = o.CorrectionRowsInserted;
                cmd.Parameters.Add("@CorrectionRowsDeleted", System.Data.SqlDbType.Int).Value = o.CorrectionRowsDeleted;
                cmd.Parameters.Add("@TotalRowsInserted", System.Data.SqlDbType.Int).Value = o.TotalRowsInserted;
                cmd.Parameters.Add("@TotalRowsDifference", System.Data.SqlDbType.Int).Value = o.TotalRowsDifference;
                cmd.Parameters.Add("@Duration", System.Data.SqlDbType.BigInt).Value = o.Duration;
                cmd.Parameters.Add("@ConsistencyFixDuration", System.Data.SqlDbType.BigInt).Value = o.ConsistencyFixDuration;
                cmd.Parameters.Add("@ConsistencyFastFixDuration", System.Data.SqlDbType.BigInt).Value = o.ConsistencyFastFixDuration;

                if (o.StatusEnum == StatusEnum.Error && !string.IsNullOrEmpty(o.ErrorMessage))
                {
                    cmd.Parameters.Add("@ErrorMessage", System.Data.SqlDbType.VarChar, 255).Value = o.ErrorMessage;
                    query.Append("ErrorMessage = @ErrorMessage,");
                }
                else query.Append("ErrorMessage = NULL,");

                query.Append("Modified = @Now, ProgressModified = @Now");
                cmd.Parameters.Add("@Now", System.Data.SqlDbType.DateTime).Value = now;
                cmd.CommandText = query.Append($" WHERE { LOGTABLEIDFIELDNAME} = @Name").ToString(); ;

                if ((int)cmd.ExecuteNonQuery() < 1) throw new Exception($"Failed to update entry {o?.Name}");

                var cmdi = new SqlCommand($@"INSERT INTO {LOGTABLELOGSQLNAME} (
                        [Name],
                        [Connection],
                        [Status],
                        [KeyIntegrityStatus],
                        [ConsitencyFixStatus],
                        [ConsitencyFastFixStatus],
                        [SourceCount],
                        [TargetCount],
                        [RowsInserted],
                        [RowsUpdated],
                        [CorrectionRowsInserted],
                        [CorrectionRowsDeleted],
                        [TotalRowsInserted],
                        [TotalRowsDifference],
                        [Duration],
                        [ConsistencyFixDuration],
                        [ConsistencyFastFixDuration],
                        [ErrorMessage],
                        [Created] 
                    ) VALUES (
                        @Name,
                        @Connection,
                        @Status,
                        @KeyIntegrityStatus,
                        @ConsitencyFixStatus,
                        @ConsitencyFastFixStatus,
                        @SourceCount,
                        @TargetCount,
                        @RowsInserted,
                        @RowsUpdated,
                        @CorrectionRowsInserted,
                        @CorrectionRowsDeleted,
                        @TotalRowsInserted,
                        @TotalRowsDifference,
                        @Duration,
                        @ConsistencyFixDuration,
                        @ConsistencyFastFixDuration,
                        @ErrorMessage,
                        @Created
                    )", this.Connection);

                cmdi.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = o.Name;
                cmdi.Parameters.Add("@Connection", System.Data.SqlDbType.VarChar, 20).Value = o.Name;
                cmdi.Parameters.Add("@Status", System.Data.SqlDbType.Int).Value = o.Status;
                cmdi.Parameters.Add("@KeyIntegrityStatus", System.Data.SqlDbType.Int).Value = o.KeyIntegrityStatus;
                cmdi.Parameters.Add("@ConsitencyFixStatus", System.Data.SqlDbType.Int).Value = o.ConsitencyFixStatus;
                cmdi.Parameters.Add("@ConsitencyFastFixStatus", System.Data.SqlDbType.Int).Value = o.ConsitencyFastFixStatus;
                cmdi.Parameters.Add("@SourceCount", System.Data.SqlDbType.Int).Value = o.SourceCount;
                cmdi.Parameters.Add("@TargetCount", System.Data.SqlDbType.Int).Value = o.TargetCount;
                cmdi.Parameters.Add("@RowsInserted", System.Data.SqlDbType.Int).Value = o.RowsInserted;
                cmdi.Parameters.Add("@RowsUpdated", System.Data.SqlDbType.Int).Value = o.RowsUpdated;
                cmdi.Parameters.Add("@CorrectionRowsInserted", System.Data.SqlDbType.Int).Value = o.CorrectionRowsInserted;
                cmdi.Parameters.Add("@CorrectionRowsDeleted", System.Data.SqlDbType.Int).Value = o.CorrectionRowsDeleted;
                cmdi.Parameters.Add("@TotalRowsInserted", System.Data.SqlDbType.Int).Value = o.TotalRowsInserted;
                cmdi.Parameters.Add("@TotalRowsDifference", System.Data.SqlDbType.Int).Value = o.TotalRowsDifference;
                cmdi.Parameters.Add("@Duration", System.Data.SqlDbType.BigInt).Value = o.Duration;
                cmdi.Parameters.Add("@ConsistencyFixDuration", System.Data.SqlDbType.BigInt).Value = o.ConsistencyFixDuration;
                cmdi.Parameters.Add("@ConsistencyFastFixDuration", System.Data.SqlDbType.BigInt).Value = o.ConsistencyFastFixDuration;
                cmdi.Parameters.Add("@ErrorMessage", System.Data.SqlDbType.VarChar, 255).Value = o.StatusEnum == StatusEnum.Error && !string.IsNullOrEmpty(o.ErrorMessage) ? (object)o.ErrorMessage : (object)DBNull.Value;
                cmdi.Parameters.Add("@Created", System.Data.SqlDbType.DateTime).Value = now;

                if ((int)cmdi.ExecuteNonQuery() < 1) throw new Exception($"Failed to insert log entry {o?.Name}");
            }
            catch
            {
               throw;
            }
        }

        public void UpdateProgress(ImportStatus o)
        {
            if (!this.initialized) throw new Exception("Not initialized");

            try
            {
                // if not exist try insert new
                if (!this.Exists(o)) this.InsertNewRow(o);

                // now update status
                var cmd = new SqlCommand($@"UPDATE {LOGTABLENAME} SET 
                    ProgressStatus = 1,
                    Progress = @Progress,
                    ProgressedRows = @ProgressedRows,
                    ProgressModified = @Now
                    WHERE {LOGTABLEIDFIELDNAME} = @Name", this.Connection);
                cmd.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = o.Name;
                cmd.Parameters.Add("@ProgressStatus", System.Data.SqlDbType.Int).Value = o.ProgressStatus;
                cmd.Parameters.Add("@Progress", System.Data.SqlDbType.Int).Value = o.Progress;
                cmd.Parameters.Add("@ProgressedRows", System.Data.SqlDbType.Int).Value = o.ProgressedRows;
                cmd.Parameters.Add("@Now", System.Data.SqlDbType.DateTime).Value = DateTime.Now;
                if ((int)cmd.ExecuteNonQuery() < 1) throw new Exception($"Import Status Logger, Failed to update progress for entry {o?.Name}");
            }
            catch
            {
                throw;
            }
        }

        private bool Exists(ImportStatus o)
        {
            return this.Exists(o.Name);
        }

        private bool Exists(string name)
        {
            var cmd = new SqlCommand($"SELECT COUNT(*) FROM {LOGTABLESQLNAME} WHERE {LOGTABLEIDFIELDNAME} = @Name", this.Connection);
            cmd.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = name;
            var exists = ((int)cmd.ExecuteScalar()) > 0;
            return exists;
        }

        private void InsertNewRow(ImportStatus o)
        {
            this.InsertNewRow(o.Name, o.Connection);
        }

        private void InsertNewRow(string name, string connectionName)
        {
            var cmd = new SqlCommand($"INSERT INTO {LOGTABLESQLNAME} (Name,Connection,Status,KeyIntegrityStatus,LastKeyIntegrityStatus,ConsitencyFixStatus,LastConsitencyFixStatus,ConsitencyFastFixStatus,LastConsitencyFastFixStatus,ProgressStatus, Modified, ProgressModified, LastStatus, SourceCount, TargetCount, LastSourceCount, LastTargetCount, RowsInserted, RowsUpdated,  LastRowsInserted, LastRowsUpdated, CorrectionRowsInserted,LastCorrectionRowsInserted,CorrectionRowsDeleted,LastCorrectionRowsDeleted,TotalRowsInserted,LastTotalRowsInserted,TotalRowsDifference,LastTotalRowsDifference,Progress, ProgressedRows, Duration, LastDuration,ConsistencyFixDuration,LastConsistencyFixDuration,ConsistencyFastFixDuration,LastConsistencyFastFixDuration, ApplyFunction) VALUES (@Name, @Connection,0,0,0,0,0,0,0,0, @Now, @Now, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)", this.Connection);
            cmd.Parameters.Add("@Name", System.Data.SqlDbType.VarChar, 50).Value = name;
            cmd.Parameters.Add("@Connection", System.Data.SqlDbType.VarChar, 10).Value = connectionName;
            cmd.Parameters.Add("@Now", System.Data.SqlDbType.DateTime).Value = DateTime.Now;
            if ((int)cmd.ExecuteNonQuery() < 1) throw new Exception($"Failed to insert new row, {name}");
        }

        private ImportStatus ConvertDbReaderToObject(SqlDataReader rd)
        {
            var o = new ImportStatus();
            o.Name = (string)rd["Name"];
            o.Connection = (string)rd["Connection"];
            o.KeyIntegrityStatus = (int)rd["KeyIntegrityStatus"];
            o.LastKeyIntegrityStatus = (int)rd["LastKeyIntegrityStatus"];
            o.ConsitencyFixStatus = (int)rd["ConsitencyFixStatus"];
            o.LastConsitencyFixStatus = (int)rd["LastConsitencyFixStatus"];
            o.ConsitencyFastFixStatus = (int)rd["ConsitencyFastFixStatus"];
            o.LastConsitencyFastFixStatus = (int)rd["LastConsitencyFastFixStatus"];
            o.Status = (int)rd["Status"];
            o.SourceCount = (int)rd["SourceCount"];
            o.TargetCount = (int)rd["TargetCount"];
            o.RowsInserted = (int)rd["RowsInserted"];
            o.RowsUpdated = (int)rd["RowsUpdated"];
            o.CorrectionRowsInserted = (int)rd["CorrectionRowsInserted"];
            o.CorrectionRowsDeleted = (int)rd["CorrectionRowsDeleted"];
            o.TotalRowsInserted = (int)rd["TotalRowsInserted"];
            o.TotalRowsDifference = (int)rd["TotalRowsDifference"];
            o.LastCorrectionRowsInserted = (int)rd["LastCorrectionRowsInserted"];
            o.LastCorrectionRowsDeleted = (int)rd["LastCorrectionRowsDeleted"];
            o.LastTotalRowsInserted = (int)rd["LastTotalRowsInserted"];
            o.LastTotalRowsDifference = (int)rd["LastTotalRowsDifference"];
            o.ProgressStatus = (int)rd["ProgressStatus"];
            o.Progress = (int)rd["Progress"];
            o.ProgressedRows = (int)rd["ProgressedRows"];
            o.ApplyFunction = (int)rd["ApplyFunction"];
            o.ErrorMessage = rd["ErrorMessage"] != DBNull.Value ? (string)rd["ErrorMessage"] : null; ;
            o.Duration = (long)rd["Duration"];
            o.ConsistencyFixDuration = (long)rd["ConsistencyFixDuration"];
            o.ConsistencyFastFixDuration = (long)rd["ConsistencyFastFixDuration"];
            o.Modified = (DateTime)rd["Modified"];
            o.ProgressModified = (DateTime)rd["ProgressModified"];

            o.LastStatus = (int)rd["LastStatus"];
            o.LastSourceCount = (int)rd["LastSourceCount"];
            o.LastTargetCount = (int)rd["LastTargetCount"];
            o.LastRowsInserted = (int)rd["LastRowsInserted"];
            o.LastRowsUpdated = (int)rd["LastRowsUpdated"];

            o.LastErrorMessage = rd["LastErrorMessage"] != DBNull.Value ? (string)rd["LastErrorMessage"] : null; ;
            o.LastDuration = (long)rd["LastDuration"];
            o.LastConsistencyFixDuration = (long)rd["LastConsistencyFixDuration"];
            o.LastConsistencyFastFixDuration = (long)rd["LastConsistencyFastFixDuration"];
            o.LastModified = rd["LastModified"] != DBNull.Value ? (DateTime?)rd["LastModified"] : null;

            return o;
        }

        private void EnsureLogTableExists()
        {
            if (!this.IsTableExisting(LOGTABLENAME))
            {
                string createScript = string.Format(CREATESTATUSTABLESCRIPT, (this.IsAzureDwh) ? string.Empty : $",{CREATESTATUSTABLEPKCONSTRANT}");
                if (this.IsAzureDwh) { createScript += " WITH (DISTRIBUTION = HASH([Name]))"; };
                var cmd = new SqlCommand(createScript, this.Connection);
                cmd.ExecuteNonQuery();
                if (!this.IsTableExisting(LOGTABLENAME)) throw new Exception("Failed to create log status table");
            }

            if (!this.IsTableExisting(LOGTABLELOGNAME))
            {
                string createScript = string.Format(CREATESTATUSTABLELOGSCRIPT, (!this.IsAzureDwh) ? "IDENTITY(1,1)" : "IDENTITY(1,1)");
                if (this.IsAzureDwh) { createScript += " WITH (DISTRIBUTION = HASH([Name]), CLUSTERED COLUMNSTORE INDEX)"; };
                var cmd = new SqlCommand(createScript, this.Connection);
                cmd.ExecuteNonQuery();
                if (!this.IsTableExisting(LOGTABLELOGNAME)) throw new Exception("Failed to create log table");
            }
        }

        private bool IsTableExisting(string tableName)
        {
            var existcmd = new SqlCommand($"SELECT CASE WHEN EXISTS((SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{tableName}')) THEN 1 ELSE 0 END", this.Connection);
            var exist = (int)existcmd.ExecuteScalar() > 0;
            return exist;
        }
    }
}
