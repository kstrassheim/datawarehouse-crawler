using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.IntegrationTest.Helper
{
    public static class SqlHelper
    {
        public readonly static Dictionary<string, string> ConnectionStrings = new Dictionary<string, string>() {
            { "Target", "Data Source=localhost,9433;Initial Catalog=Empty;User ID=sa;Password=H5m4+?Ebbh*154" },
            { "src", "Data Source=localhost,9433;Initial Catalog=AdventureWorks2017;User ID=sa;Password=H5m4+?Ebbh*154" }
        };

        //public static readonly string SrcBakPath = Path.GetFullPath("AdventureWorks.bak");
        //public static readonly string TargetBakPath = Path.GetFullPath("Datawarehouse_UnitTest.bak");

        public static readonly int DefaultTimeout = 120;

        //public static void InitTestEnvironment()
        //{
        //    using(var con = new SqlConnection(InitConnectionString))
        //    {
        //        try
        //        {
        //            con.Open();
        //            DropAndRestoreDatabase("AdventureWorks2017", SrcBakPath, con);
        //            DropAndRestoreDatabase("Datawarehouse.UnitTest", TargetBakPath, con);
        //        }
        //        finally
        //        {
        //            con.Close();
        //        }
        //    }
        //}

        public static void DropAndRestoreDatabase(string dbName, string backupPath, SqlConnection con)
        {
            var cmdExists = new SqlCommand($"SELECT COUNT(*) FROM master.dbo.sysdatabases where name =\'{dbName}\'", con);
            var cmdRestore = new SqlCommand($"Restore Database [{dbName}] FROM DISK ='{backupPath}' WITH REPLACE", con);
            con.ChangeDatabase("master");
            var exists = ((int)cmdExists.ExecuteScalar() > 0);

            if (exists)
            {
                // set database to single user to close connections and drop it
                var sucmd = new SqlCommand($"ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", con);
                var cmdDrop = new SqlCommand($"Drop Database [{dbName}]", con);
                sucmd.ExecuteNonQuery();
                cmdDrop.ExecuteNonQuery();
            }

            cmdRestore.ExecuteNonQuery();
        }

        public static void RestoreDatabase(string dbName, SqlConnection con)
        {
            //var cmdExists = new SqlCommand($"SELECT COUNT(*) FROM master.dbo.sysdatabases where name =\'{dbName}\'", con);
            var cmdRestore = new SqlCommand($"Restore Database [{dbName}] FROM DISK ='/tmp/{dbName}.bak' WITH REPLACE", con);
            con.ChangeDatabase("master");
            cmdRestore.ExecuteNonQuery();
        }

        public static void ConnectAndRestoreDatabase(string connectionStringName)
        {
            var conStr = ConnectionStrings[connectionStringName].ToString();
            using(var con = new SqlConnection(conStr))
            {
                con.Open();
                string dbName = con.Database;
                //var cmdExists = new SqlCommand($"SELECT COUNT(*) FROM master.dbo.sysdatabases where name =\'{dbName}\'", con);
                var cmdRestore = new SqlCommand($"Restore Database [{dbName}] FROM DISK ='/tmp/{dbName}.bak' WITH REPLACE", con);
                con.ChangeDatabase("master");
                cmdRestore.ExecuteNonQuery();
                con.Close();
            }
        }

        public static bool IsTableExisting(string tableName, SqlConnection con)
        {
            var cmd = new SqlCommand($"select case when exists((select * from information_schema.tables where table_name = '{tableName}')) then 1 else 0 end", con);
            cmd.CommandTimeout = DefaultTimeout;
            var exists = (int)cmd.ExecuteScalar() == 1;
            return exists;
        }

        public static int GetRowsCount(string tableName, SqlConnection con)
        {
            var cmd = new SqlCommand($"SELECT Count(*) FROM {tableName}", con);
            cmd.CommandTimeout = DefaultTimeout;
            var count = (int)cmd.ExecuteScalar();
            return count;
        }

        public static string[] GetFieldValues(string tableName, string fieldName, string whereClause, SqlConnection con)
        {
            var cmd = new SqlCommand($"SELECT {fieldName} FROM {tableName} WHERE {whereClause}", con);
            cmd.CommandTimeout = DefaultTimeout;
            var reader = cmd.ExecuteReader();
            var res = new List<string>();
            while(reader.Read())
            {
                var obj = reader[fieldName]?.ToString();
                if (obj != null) { res.Add(obj); }
            }
            reader.Close();

            return res?.ToArray();
        }

        public static IEnumerable<string[]> GetFieldValues(string tableName, string[] fieldNames, string whereClause, SqlConnection con)
        {
            var cmd = new SqlCommand($"SELECT {string.Join(',', fieldNames)} FROM {tableName}{(!string.IsNullOrEmpty(whereClause) ? $" WHERE {whereClause}" : string.Empty)}", con);
            cmd.CommandTimeout = DefaultTimeout;
            var reader = cmd.ExecuteReader();
            var res = new List<string[]>();
            while (reader.Read())
            {
                List<string> values = new List<string>();
                foreach(string fieldName in fieldNames)
                {
                    var obj = reader[fieldName]?.ToString();
                    if (obj != null) { values.Add(obj.ToString()); }
                }
                res.Add(values.ToArray());
            }
            reader.Close();

            return res?.ToArray();
        }

        public static object[] GetFieldValueObjects(string tableName, string fieldName, string whereClause, SqlConnection con)
        {
            var cmd = new SqlCommand($"SELECT {fieldName} FROM {tableName} WHERE {whereClause}", con);
            cmd.CommandTimeout = DefaultTimeout;
            var reader = cmd.ExecuteReader();
            var res = new List<object>();
            while (reader.Read())
            {
                var obj = reader[fieldName];
                if (obj != null) { res.Add(obj); }
            }
            reader.Close();

            return res?.ToArray();
        }

        public static IEnumerable<object[]> GetFieldValueObjects(string tableName, string[] fieldNames, string whereClause, SqlConnection con)
        {
            var cmd = new SqlCommand($"SELECT {string.Join(',', fieldNames)} FROM {tableName}{(!string.IsNullOrEmpty(whereClause) ? $" WHERE {whereClause}" : string.Empty )}", con);
            cmd.CommandTimeout = DefaultTimeout;
            var reader = cmd.ExecuteReader();
            var res = new List<object[]>();
            while (reader.Read())
            {
                var values = new List<object>();
                foreach (string fieldName in fieldNames)
                {
                    var obj = reader[fieldName];
                    if (obj != null) { values.Add(obj); }
                }
                res.Add(values.ToArray());
            }
            reader.Close();

            return res?.ToArray();
        }

        public static int RunDeleteScript(string tableName, SqlConnection con, string customWhereClause = null)
        {
            var cmd = new SqlCommand($"DELETE FROM {tableName}{(!string.IsNullOrEmpty(customWhereClause) ? $" WHERE {customWhereClause}" : "")}", con);
            cmd.CommandTimeout = DefaultTimeout;
            var ct = (int)cmd.ExecuteNonQuery();
            return ct;
        }

        public static string[] getPrimaryKeys(string tableName, SqlConnection con)
        {
            var pkeys = new List<string>();
            var selColName = "ColumnName";
            var tblName = tableName.Split('.')?.Last().TrimStart('[').TrimEnd(']');

            var cmd = new SqlCommand($"SELECT i.name AS IndexName, OBJECT_NAME(ic.OBJECT_ID) AS TableName, COL_NAME(ic.OBJECT_ID,ic.column_id) AS {selColName} FROM sys.indexes AS i INNER JOIN sys.index_columns AS ic ON  i.OBJECT_ID = ic.OBJECT_ID AND i.index_id = ic.index_id WHERE i.is_primary_key = 1 AND OBJECT_NAME(ic.OBJECT_ID) = '{tblName}'", con);
            cmd.CommandTimeout = DefaultTimeout;
            var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                pkeys.Add(rd[selColName].ToString());
            }
            rd.Close();

            return pkeys.ToArray();
        }

        public static void ExecuteSqlCommand(string sqlCmd, SqlConnection con)
        {
            var cmd = new SqlCommand(sqlCmd, con);
            cmd.CommandTimeout = 36000;
            cmd.ExecuteNonQuery();
        }

        public static void ExecuteSqlFile(string sqlFile, SqlConnection con)
        {
            var sql = string.Empty;
            using (var reader = new StreamReader(sqlFile))
            {
                sql = reader.ReadToEnd();
            }

            var regex = new Regex("^GO", RegexOptions.IgnoreCase | RegexOptions.Multiline);
            sql = regex.Replace(sql, string.Empty);
            var cmd = new SqlCommand(sql, con);
            cmd.CommandTimeout = 36000;
            cmd.ExecuteNonQuery();
        }

        public static void RunConnectionBlock(Action<SqlConnection, SqlConnection> fkt)
        {
            using (SqlConnection src = new SqlConnection(ConnectionStrings["src"]), tgt = new SqlConnection(ConnectionStrings["Target"]))
            {
                try
                {
                    src.Open();
                    tgt.Open();
                    fkt(src, tgt);
                }
                finally
                {
                    src.Close();
                    tgt.Close();
                }
            }
        }

        public static void RunTargetConnectionBlock(Action<SqlConnection> fkt)
        {
            using (var tgt = new SqlConnection(ConnectionStrings["Target"]))
            {
                try
                {
                    tgt.Open();
                    fkt(tgt);
                }
                finally
                {
                    tgt.Close();
                }
            }
        }
    }
}
