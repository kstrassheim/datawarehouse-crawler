using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.QueryAdapters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.DataAdapters
{
    public enum TransactionModeEnum
    {
        None = 0, ReadOnly = 1, Write = 2
    }

    public enum ConnectionModeEnum
    {
        None = 0, OpenClose = 1, OpenCloseDispose = 2
    }

    public class SqlDataAdapter : ITargetDataAdapter, ISourceDataAdapter
    {
        #region nested types

        internal enum DistributionType { Hash = 0, RoundRobin = 1, Replicated = 2 }

        internal class AzureDwhSettings
        {
            public bool Columnstore { get; set; }
            public DistributionType DistributionType { get; set; }

            public string DistributionColumn { get; set; }

            public bool ExplizitIgnoreIdentity { get; set; }
        }

        internal class SqlTableCreator
        {
            private bool initialized;

            protected SqlConnection Connection { get; set; }

            protected SqlTransaction Transaction { get; set; }

            protected string TargetTableName { get; set; }

            protected DataTable SourceTable { get; set; }

            protected string[] PreferredPrimaryKeys { get; set; }

            protected Dictionary<string, ColumnTypeInfo> OrgColumnTypes { get; set; }

            protected AzureDwhSettings AzureDwhSettings { get; set; }

            public int Result { get; private set; }

            public bool AvoidCreatingPrimaryKeysInSchema { get; set; } = false;

            public bool AddIgnorePrimaryKeyErrorsInOnPremMode { get; set; } = true;

            public SqlTableCreator() { }

            public SqlTableCreator(string targetTableName, DataTable sourceTable, string[] preferredPrimaryKeys, Dictionary<string, ColumnTypeInfo> orgColumnTypes, AzureDwhSettings azureDwhSettings, SqlConnection connection, SqlTransaction transaction = null)
            {
                this.Init(targetTableName, sourceTable, preferredPrimaryKeys, orgColumnTypes, azureDwhSettings, connection, transaction);
            }


            public void Init(string tableName, DataTable sourceTable, string[] preferredPrimaryKeys, Dictionary<string, ColumnTypeInfo> orgColumnTypes, AzureDwhSettings azureDwhSettings, SqlConnection connection, SqlTransaction transaction = null)
            {
                this.TargetTableName = tableName;
                this.SourceTable = sourceTable;
                this.PreferredPrimaryKeys = preferredPrimaryKeys;
                this.OrgColumnTypes = orgColumnTypes;
                this.AzureDwhSettings = azureDwhSettings;
                this.Connection = connection;
                this.Transaction = transaction;
                this.initialized = true;
            }

            public void Run()
            {
                if (!initialized) throw new Exception("Cannot run before initialized");

                // generate command
                List<string> colLines = new List<string>();
                var pkString = string.Empty;

                // primary keys
                List<string> appliedPks = new List<string>();
                string appliedPkArg = string.Empty;

                // use preferren pks
                if (this.PreferredPrimaryKeys != null && this.PreferredPrimaryKeys.Length > 0)
                {
                    appliedPks.AddRange(this.PreferredPrimaryKeys.Select(o => $"[{o.Trim().TrimStart('[').TrimEnd(']').Trim()}]"));
                }
                // if no primary keys selected use table pks
                else if (this.SourceTable.PrimaryKey.Length > 0)
                {
                    appliedPks.AddRange(this.SourceTable.PrimaryKey.Select(column => $"[{column.ColumnName}]"));
                }

                if (appliedPks.Count > 0)
                {
                    appliedPkArg = string.Join(", ", appliedPks);
                    // only create pk keys when not azure dwh
                    if (this.AzureDwhSettings == null && !this.AvoidCreatingPrimaryKeysInSchema)
                    {
                        pkString = new StringBuilder($"\tCONSTRAINT [PK_{this.TargetTableName}] PRIMARY KEY CLUSTERED (").Append(appliedPkArg).Append(")").Append(this.AddIgnorePrimaryKeyErrorsInOnPremMode ? " WITH (IGNORE_DUP_KEY = ON)" : string.Empty).ToString();

                    }
                }

                string azureDwhSuffix = null;
                string distributionCol = string.Empty;
                if (this.AzureDwhSettings != null)
                {
                    var withLines = new List<string>();

                    // only apply hash when 1 primary key present
                    if ((!string.IsNullOrEmpty(this.AzureDwhSettings.DistributionColumn) || appliedPks.Count() > 0) && this.AzureDwhSettings.DistributionType != DistributionType.Replicated)
                    {
                        distributionCol = !string.IsNullOrEmpty(this.AzureDwhSettings.DistributionColumn) ? $"{this.AzureDwhSettings.DistributionColumn.Trim().TrimStart('[').TrimEnd(']').Trim()}" : appliedPks.First()?.Trim().TrimStart('[').TrimEnd(']').Trim();
                        if (distributionCol.Contains(',')) throw new ArgumentException($"Error creating AzureDWH {this.TargetTableName}: Distribution Column can be only one, remove comma from settings");
                        withLines.Add($"DISTRIBUTION = HASH ([{distributionCol}])");
                    }
                    else if (this.AzureDwhSettings.DistributionType == DistributionType.Replicated)
                    {
                        withLines.Add($"DISTRIBUTION = REPLICATE");
                    }
                    else
                    {
                        withLines.Add($"DISTRIBUTION = ROUND_ROBIN");
                    }

                    // apply distribution type


                    // Only apply clustered index when no columnstore selected and more than 1 
                    if (!this.AzureDwhSettings.Columnstore && appliedPks.Count() > 0 && !this.AzureDwhSettings.ExplizitIgnoreIdentity)
                    {
                        withLines.Add($"CLUSTERED INDEX ({appliedPkArg})");
                    }
                    else
                    {
                        withLines.Add($"CLUSTERED COLUMNSTORE INDEX");
                    }

                    if (withLines.Count > 0)
                    {
                        azureDwhSuffix = $" WITH ({string.Join(",\n", withLines)})";
                    }
                }

                var typeMap = new Dictionary<Type, SqlDbType>() {
                        {typeof(string), SqlDbType.NVarChar },
                        {typeof(char[]), SqlDbType.NVarChar },
                        {typeof(int), SqlDbType.Int },
                        {typeof(Int16), SqlDbType.SmallInt },
                        {typeof(Int64),SqlDbType.BigInt },
                        {typeof(Byte[]),SqlDbType.VarBinary },
                        {typeof(Boolean),SqlDbType.Bit },
                        {typeof(DateTime),SqlDbType.DateTime2 },
                        {typeof(DateTimeOffset),SqlDbType.DateTimeOffset },
                        {typeof(Decimal),SqlDbType.Decimal },
                        {typeof(Double),SqlDbType.Float },
                        {typeof(Byte),SqlDbType.TinyInt },
                        {typeof(TimeSpan),SqlDbType.Time },
                        {typeof(Guid),SqlDbType.UniqueIdentifier }
                    };

                // columns
                foreach (DataColumn column in this.SourceTable.Columns)
                {
                    if (!typeMap.ContainsKey(column.DataType)) throw new Exception($"{column.DataType.ToString()} not possible to map to sql db type.");
                    var sqlType = typeMap[column.DataType];

                    string typeStr = null;
                    if (column.DataType == typeof(string))
                    {
                        typeStr = $"{sqlType.ToString("f").ToUpper()}({column.MaxLength})";
                    }
                    else if (column.DataType == typeof(DateTime))
                    {
                        if (this.OrgColumnTypes.ContainsKey(column.ColumnName))
                        {
                            var orgtype = this.OrgColumnTypes[column.ColumnName];
                            if (orgtype.TypeName.ToLower() == "datetime2" && orgtype.Precision != null)
                            {
                                typeStr = $"{orgtype.TypeName}({orgtype.Precision.Value})";
                            }
                            else
                            {
                                typeStr = $"{orgtype.TypeName}";
                            }
                        }
                        else
                        {
                            typeStr = $"{sqlType.ToString("f").ToUpper()}";
                        }
                    }
                    else if (column.DataType == typeof(decimal))
                    {
                        if (this.OrgColumnTypes.ContainsKey(column.ColumnName))
                        {
                            var orgtype = this.OrgColumnTypes[column.ColumnName];
                            if (orgtype.TypeName.ToLower() == "money" || orgtype.TypeName.ToLower() == "smallmoney")
                            {
                                typeStr = $"{orgtype.TypeName}";
                            }
                            else
                            {
                                int prec = orgtype.Precision != null ? orgtype.Precision.Value : 19;
                                int scale = orgtype.Scale != null ? orgtype.Scale.Value : 4;
                                typeStr = $"{orgtype.TypeName}({prec}, {scale})";
                            }
                        }
                        else
                        {
                            typeStr = $"{sqlType.ToString("f").ToUpper()}(19, 4)";
                        }
                    }
                    else
                    {
                        typeStr = $"{sqlType.ToString("f").ToUpper()}";
                    }
                    
                    //typeStr = $"{sqlType.ToString("f").ToUpper()}{(column.DataType == typeof(string) ? $"({column.MaxLength})" : string.Empty)}";

                    var fullStr = $"\t[{column.ColumnName}] {typeStr}";

                    // in azure dwh and when single pk not distribution column and type int add Identity
                    if (this.AzureDwhSettings != null &&
                        appliedPks.Count() == 1 &&
                        column.ColumnName != distributionCol &&
                        column.ColumnName == appliedPks[0].Trim().TrimStart('[').TrimEnd(']').Trim() &&
                        (column.DataType == typeof(int) || column.DataType == typeof(long)) &&
                        !this.AzureDwhSettings.ExplizitIgnoreIdentity
                    )
                    {
                        fullStr += " IDENTITY(1,1)";
                    }

                    if (!column.AllowDBNull) { fullStr += " NOT NULL"; }

                    colLines.Add(fullStr);
                }


                if (!string.IsNullOrEmpty(pkString)) colLines.Add(pkString);
                var sql = $"CREATE TABLE [dbo].[{this.TargetTableName}] ({string.Join(",\n", colLines)})";

                if (!string.IsNullOrEmpty(azureDwhSuffix)) { sql += azureDwhSuffix; }

                // run command
                var cmd = (this.Transaction != null && this.Transaction.Connection != null) ? new SqlCommand(sql, this.Connection, this.Transaction) : new SqlCommand(sql, this.Connection);

                // run
                this.Result = cmd.ExecuteNonQuery();
            }
        }

        #endregion nested types

        #region props

        public uint ParameterLimit { get; set; } = 2000;

        public int? CustomPageSize { get; private set; }

        public TransactionModeEnum TransactionMode { get; set; }

        public ConnectionModeEnum ConnectionMode { get; set; }

        public bool IsAzureDwh { get; set; }

        public int SqlCmdTimeout { get; set; }

        protected int SqlVersion { get; set; }

        public Table Table { get; protected set; }

        protected SqlConnection Connection { get; set; }

        protected SqlTransaction Transaction { get; set; }

        private SqlQueryAdapter queryAdapter = new SqlQueryAdapter();

        protected SqlQueryAdapter QueryAdapter { get { return this.queryAdapter; } }

        public bool ForceOldSqlVersion { 
            get { return this.QueryAdapter.ForceOldSqlVersion; }
            set { this.QueryAdapter.ForceOldSqlVersion = value; }
        }

        public bool ReadJoinFieldsFromLocalCopy
        {
            get { return this.QueryAdapter.ReadJoinFieldsFromLocalCopy; }
            set { this.QueryAdapter.ReadJoinFieldsFromLocalCopy = value; }
        }

        public bool AvoidCreatingPrimaryKeysInSchema { get; set; } = false;

        public bool AddIgnorePrimaryKeyErrorsInOnPremMode { get; set; } = true;

        #endregion props

        public SqlDataAdapter(Table table, SqlConnection connection, bool isAzureDwh = false, ConnectionModeEnum connectionMode = ConnectionModeEnum.None, TransactionModeEnum transactionMode = TransactionModeEnum.None)
        {
            this.Table = table;
            this.IsAzureDwh = isAzureDwh;
            this.Connection = connection;
            this.ConnectionMode = connectionMode;
            this.TransactionMode = transactionMode;
            this.Init();
        }

        #region methods

        public void Init()
        {
            if (this.ConnectionMode > ConnectionModeEnum.None && this.Connection.State != ConnectionState.Open) { this.Connection.Open(); }
            if (this.TransactionMode > TransactionModeEnum.None) { 
                this.Transaction = this.Connection.BeginTransaction(); 
            }
            this.SqlVersion = this.QueryAdapter.SqlVersion = this.IsAzureDwh ? 1 : this.TryGetServerMajorVersion(this.Connection);
        }

        public void Dispose()
        {
            if (this.Transaction != null && this.TransactionMode == TransactionModeEnum.ReadOnly){ this.Transaction.Rollback(); }
            else if (this.Transaction != null && this.TransactionMode == TransactionModeEnum.Write) { this.Transaction.Commit(); }
            if (this.ConnectionMode > ConnectionModeEnum.None && this.Connection.State != ConnectionState.Closed) { this.Connection.Close(); }
            if (this.ConnectionMode == ConnectionModeEnum.OpenCloseDispose) { this.Connection.Dispose(); }
           
        }

        public Task<object> GetCountAsync(Condition filter = null)
        {
            var query = new SelectQuery(new Column[] { new CountRowsColumn() }, this.Table, filter);
            var qs = this.QueryAdapter.ConvertSelectQuery(query);
            var cmd = new SqlCommand(qs, this.Connection, this.Transaction);
            var parameters = this.QueryAdapter.GetParametersForCondition(query.Condition);
            cmd.Parameters.AddRange(parameters?.ToArray() ?? new SqlParameter[0]);
            cmd.CommandTimeout = this.SqlCmdTimeout;
            //object res = cmd.ExecuteScalar();
            return cmd.ExecuteScalarAsync();
        }

        public int GetCount(Condition filter = null)
        {
            var query = new SelectQuery(new Column[] { new CountRowsColumn() }, this.Table, filter);
            var qs = this.QueryAdapter.ConvertSelectQuery(query);
            var cmd = new SqlCommand(qs, this.Connection, this.Transaction);
            var parameters = this.QueryAdapter.GetParametersForCondition(query.Condition);
            cmd.Parameters.AddRange(parameters?.ToArray() ?? new SqlParameter[0]);
            cmd.CommandTimeout = this.SqlCmdTimeout;
            return (int)cmd.ExecuteScalar();
        }

        public IEnumerable<IEnumerable<object>> GetFieldValues(SelectQuery selectQuery)
        {
            List<List<object>> ret = new List<List<object>>();
            var query = this.QueryAdapter.ConvertSelectQuery(selectQuery);
            var parameters = this.QueryAdapter.GetParametersForCondition(selectQuery.Condition);
            var cmd = new SqlCommand(query, this.Connection, this.Transaction) { CommandTimeout = SqlCmdTimeout };
            if (parameters != null) {  foreach (var p in parameters) { cmd.Parameters.Add(p); } }
            var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var r = new List<object>();
                var len = this.queryAdapter.UseOldSqlQuery(selectQuery) ? rd.FieldCount - 1 : rd.FieldCount;
                for (var i = 0; i < len; i++)
                {
                    r.Add(rd.GetValue(i));
                }
                ret.Add(r);
            }

            rd.Close();
            return ret;
        }

        public Task<IEnumerable<IEnumerable<object>>> GetFieldValuesAsync(SelectQuery selectQuery)
        {
            var t = new Task<IEnumerable<IEnumerable<object>>>(() => this.GetFieldValues(selectQuery));
            t.Start();
            return t;
        }

        public IEnumerable<IEnumerable<object>> GetFieldValues(IEnumerable<Column> fields, Condition condition = null)
        {
            return this.GetFieldValues(new SelectQuery(fields, this.Table, condition));
        }

        public object GetMaxFieldValue(Column sourceField, Condition c = null)
        {
            var query = sourceField.GetType() == typeof(JoinedColumn) ?
               this.QueryAdapter.GetMaxValueQuery(new JoinedMaxValueColumn((JoinedColumn)sourceField), this.Table) :
               this.QueryAdapter.GetMaxValueQuery(new MaxValueColumn(sourceField), this.Table, c);
            var cmdSource = new SqlCommand(query, this.Connection, this.Transaction) { CommandTimeout = SqlCmdTimeout, Transaction = this.Transaction };
            var param = this.queryAdapter.GetParametersForCondition(c)?.ToArray();
            if (param != null) { cmdSource.Parameters.AddRange(param); }
            object queryResultSource = cmdSource.ExecuteScalar();
            return queryResultSource;
        }

        public DataTable GetSchema(IEnumerable<Column> columns = null)
        {
            var cols = columns != null ? this.QueryAdapter.EscapeColumnsForQuery(columns) : "*";
            var joins = columns?.Where(c => c.GetType() == typeof(JoinedColumn))?.GroupBy(c => ((JoinedColumn)c).Model.SourceName);
            var tableJoinQuery = this.QueryAdapter.GetTableJoinQuery(columns);
            //if (joins != null) { selectColumns += $",{joins.SelectColumns}"; }
            var ds = new DataSet();
            using (var ad = new System.Data.SqlClient.SqlDataAdapter($"SELECT TOP(1) {cols} FROM {this.Table.Name} {this.Table.Alias} {(!string.IsNullOrEmpty(tableJoinQuery) ? tableJoinQuery : string.Empty)}", this.Connection))
            {
                ad.SelectCommand.Transaction = this.Transaction;
                ad.FillSchema(ds, SchemaType.Source);
            }
            
            return ds.Tables[0];
        }

        public IEnumerable<ColumnTypeInfo> GetExtendedColumnTypeInfo(string tableName = null)
        {
            var ret = new List<ColumnTypeInfo>();
            var query = "SELECT COLUMN_NAME, DATA_TYPE , NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p";
            var cmd = new SqlCommand(query, this.Connection, this.Transaction) { CommandTimeout = SqlCmdTimeout, Transaction = this.Transaction };
            var tn = !string.IsNullOrEmpty(tableName) ? tableName : this.Table.Name;
            if (tn.Contains('.'))
            {
                tn = tn.Split('.').Last();
            }
            cmd.Parameters.AddWithValue("@p", tn);
            var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var ci = new ColumnTypeInfo();
                ci.Name = rd[0]?.ToString();
                ci.TypeName = rd[1]?.ToString();

                if (int.TryParse(rd[2]?.ToString(), out int pr))
                {
                    ci.Precision = pr;
                }

                if (int.TryParse(rd[3]?.ToString(), out int sc))
                {
                    ci.Scale = sc;
                }

                ret.Add(ci);
            }

            rd.Close();
            return ret;
        }

        public DataSet RunQuery(SelectQuery query)
        {
            var ad = new System.Data.SqlClient.SqlDataAdapter() { SelectCommand = new SqlCommand(this.QueryAdapter.ConvertSelectQuery(query), this.Connection, this.Transaction) { CommandTimeout = this.SqlCmdTimeout } };
            var parameters = this.QueryAdapter.GetParametersForCondition(query.Condition);
            if (parameters != null && parameters.Count() > 0) { foreach (var p in parameters) { ad.SelectCommand.Parameters.Add(p); } }
            var ds = new DataSet();
            ad.Fill(ds);

            if (ds.Tables[0].Columns.Contains(this.QueryAdapter.CUSTOMROWNUMBERCOLNAME)) { ds.Tables[0].Columns.Remove(this.QueryAdapter.CUSTOMROWNUMBERCOLNAME); }
            return ds;
        }

        public Task<DataSet> RunQueryAsync(SelectQuery query)
        {
            Task<DataSet> t = new Task<DataSet>(() => { return this.RunQuery(query); });
            t.Start();
            return t;
        }

        public Task<int> RunUpdateAsync(UpdateQuery updateQuery)
        {
            var query = this.QueryAdapter.ConvertUpdateQuery(updateQuery);
            var parameters = this.QueryAdapter.GetParametersForUpdateQuery(updateQuery);

            var cmd = new SqlCommand();
            cmd.Parameters.AddRange(parameters.ToArray());
            cmd.Connection = this.Connection;
            cmd.Transaction = this.Transaction;
            cmd.CommandTimeout = this.SqlCmdTimeout;
            cmd.CommandText = query;
            return cmd.ExecuteNonQueryAsync();
        }

        public async Task<int> RunUpdateAsync(IEnumerable<UpdateQuery> updateQuery)
        {
            var ret = 0;
            if (updateQuery == null || updateQuery.Count() < 1) { var t = new Task<int>(()=>ret); t.Start();return await t; }
            // get first element
            var fu = updateQuery.First();
            var first = this.QueryAdapter.ConvertUpdateQuery(fu, $"r{0}");
            var firstParams = this.QueryAdapter.GetParametersForUpdateQuery(fu, $"r{0}");
            // calculate page size
            var plc = firstParams.Count() > 0 ? (uint)firstParams.Count() : this.ParameterLimit;
            var pageSize = this.ParameterLimit / plc - 1;
            int current = 0;

            var ucmd = new StringBuilder();
            var uparams = new List<SqlParameter>();

            Action<string, IEnumerable<SqlParameter>> appendQuery = (uq, upl) =>
            {
                ucmd.Append(uq).Append(";\n");
                uparams.AddRange(upl);
                current++;
            };

            Func<Task> executePage = async () =>
            {
                if (current < 1) { return; }
                var cmd = new SqlCommand();
                cmd.Parameters.AddRange(uparams.ToArray());
                cmd.Connection = this.Connection;
                cmd.Transaction = this.Transaction;
                cmd.CommandTimeout = this.SqlCmdTimeout;
                cmd.CommandText = ucmd.ToString();
                ret += await cmd.ExecuteNonQueryAsync();
                ucmd.Clear();
                uparams.Clear();
                current = 0;
            };

            appendQuery(first, firstParams);

            for (var i = 1; i < updateQuery.Count(); i++)
            {
                appendQuery(this.QueryAdapter.ConvertUpdateQuery(updateQuery.ElementAt(i), $"r{current}"), this.QueryAdapter.GetParametersForUpdateQuery(updateQuery.ElementAt(i), $"r{current}"));
                
                if (current >= pageSize)
                {
                    await executePage();
                }
            }

            await executePage();

            return ret;
        }

        private int TryGetServerMajorVersion(SqlConnection con)
        {
            try
            {
                int version = 0;
                if (!string.IsNullOrEmpty(con.ServerVersion) && int.TryParse(con.ServerVersion.Split('.')[0], out version))
                {
                    return version;
                }
            }
            catch { }
            return 0;
        }

        public bool TableExists()
        {
            var q = $"SELECT CASE WHEN EXISTS((SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{this.Table.Name}')) THEN 1 ELSE 0 END";
            var cmd = new SqlCommand(q, this.Connection, this.Transaction);
            cmd.CommandTimeout = this.SqlCmdTimeout;
            var exists = (int)cmd.ExecuteScalar() == 1;
            return exists;
        }

        public void DeleteTable()
        {
            var deltblcmd = new SqlCommand($"DROP TABLE {this.Table.Name}", this.Connection, this.Transaction);
            deltblcmd.CommandTimeout = this.SqlCmdTimeout;
            deltblcmd.ExecuteNonQuery();
        }

        public int DeleteData()
        {
            var cmd = new SqlCommand($"DELETE FROM {this.Table.Name}", this.Connection, this.Transaction);
            cmd.CommandTimeout = SqlCmdTimeout;
            return (int)cmd.ExecuteNonQuery();
        }

        public int Delete(Condition condition)
        {
            if (condition == null) { throw new ArgumentException("An Null condition is not accepable for delete command"); }
            var conditionQuery = this.QueryAdapter.ConvertCondition(condition, 0, true);
            var parameters = this.QueryAdapter.GetParametersForCondition(condition);
            var cmd = new SqlCommand($"DELETE FROM {this.Table.Name} WHERE {conditionQuery}", this.Connection, this.Transaction);
            cmd.Parameters.AddRange(parameters.ToArray());
            cmd.CommandTimeout = SqlCmdTimeout;
            return (int)cmd.ExecuteNonQuery();
        }

        public virtual bool CreateTable(DataTable schema, IEnumerable<Column> idColumns, IEnumerable<Column> allColumns, ExpectedSizeEnum expectedSize, string distributionColumn = null, bool azureDwhIgnoreIdentity = false)
        {
            AzureDwhSettings azureDwhSet = null;

            if (this.IsAzureDwh)
            {
                azureDwhSet = new AzureDwhSettings()
                {
                    //define settings
                    // apply replicated distribution for small and medium tables
                    DistributionType = expectedSize > ExpectedSizeEnum.big ? DistributionType.Replicated : DistributionType.Hash,
                    // apply columnstore for tables bigger than medium
                    Columnstore = expectedSize < ExpectedSizeEnum.medium,

                    DistributionColumn = distributionColumn,

                    ExplizitIgnoreIdentity = azureDwhIgnoreIdentity

                };
            }
            var orgTypeInfo = allColumns.Where(i => i.OriginalColumnTypeInfo != null).Select(o => o.OriginalColumnTypeInfo).ToDictionary((cti) => cti.Name);
            var c = new SqlTableCreator(this.Table.Name, schema, idColumns.Select(o => o.Name).ToArray(), orgTypeInfo, azureDwhSet, this.Connection)
            {
                AvoidCreatingPrimaryKeysInSchema = this.AvoidCreatingPrimaryKeysInSchema,
                AddIgnorePrimaryKeyErrorsInOnPremMode = this.AddIgnorePrimaryKeyErrorsInOnPremMode
            };

            c.Run();
            return this.TableExists();
        }

        public void Insert(DataTable table, int pageSize, Action<long> statusCallback = null) {
            var bulkCopy = new SqlBulkCopy(this.Connection, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, this.Transaction);
            bulkCopy.DestinationTableName = this.Table.Name;
            bulkCopy.BulkCopyTimeout = this.SqlCmdTimeout;
            bulkCopy.NotifyAfter = pageSize / 10;
            bulkCopy.SqlRowsCopied += delegate (object sender, SqlRowsCopiedEventArgs e)
            {
                statusCallback?.Invoke(e.RowsCopied);
            };
            bulkCopy.WriteToServer(table);
        }

        public async Task InsertAsync(DataTable table, int pageSize, Action<long> statusCallback = null)
        {
            var bulkCopy = new SqlBulkCopy(this.Connection, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, this.Transaction);
            bulkCopy.DestinationTableName = this.Table.Name;
            bulkCopy.BulkCopyTimeout = this.SqlCmdTimeout;
            bulkCopy.NotifyAfter = pageSize / 10;
            bulkCopy.SqlRowsCopied += delegate (object sender, SqlRowsCopiedEventArgs e)
            {
                statusCallback?.Invoke(e.RowsCopied);
            };

            await bulkCopy.WriteToServerAsync(table);
        }

        public bool HasDuplicateKeys(IEnumerable<Column> idColumns)
        {
            var innerq = this.QueryAdapter.ConvertSelectQuery(new SelectQuery(idColumns, this.Table, null));
            var q = $"SELECT Count(*) FROM ({innerq} GROUP BY {this.QueryAdapter.EscapeColumnsForQuery(idColumns)} HAVING Count(*) > 1) as checkintegrity";
            var cmd = new SqlCommand(q, this.Connection, this.Transaction);
            cmd.CommandTimeout = this.SqlCmdTimeout;
            var exists = (int)cmd.ExecuteScalar() > 0;

            return exists;
        }

        public IEnumerable<IEnumerable<object>> GetDublicateKeys(IEnumerable<Column> idColumns)
        {
            List<List<object>> ret = new List<List<object>>();
            var query = $"{this.QueryAdapter.ConvertSelectQuery(new SelectQuery(idColumns, this.Table, null))} GROUP BY {this.QueryAdapter.EscapeColumnsForQuery(idColumns)} HAVING Count(*) > 1";
            var cmd = new SqlCommand(query, this.Connection, this.Transaction) { CommandTimeout = SqlCmdTimeout };
            var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var r = new List<object>();
                for (var i = 0; i < rd.FieldCount; i++)
                {
                    r.Add(rd.GetValue(i));
                }
                ret.Add(r);
            }

            rd.Close();
            return ret;
        }


        private IEnumerable<IEnumerable<T>> splitList<T>(IEnumerable<T> topList, int splitSize)
        {
            if (splitSize <= 0) { throw new ArgumentException("Split size cannot be above 1"); }
            var ret = new List<IEnumerable<T>>();
            var j = 0; var l = new List<T>();
            foreach (var el in topList)
            {
                if (j >= splitSize)
                {
                    ret.Add(l);
                    l = new List<T>();
                    j = 0;
                }

                l.Add(el);
                j++;
            }

            ret.Add(l);

            return ret;
        }

        public IEnumerable<Condition> GetConditionsForValues(IEnumerable<Column> cols, IEnumerable<IEnumerable<object>> values, CompareSymbol cs = CompareSymbol.eq)
        {
            var conds = new List<Condition>();
            if (values.Count() < 1) { return conds; }
            var innerConnectSymbol = cs == CompareSymbol.eq ? ConnectSymbols.and : ConnectSymbols.or;
            var outerConnectSymbol = cs == CompareSymbol.eq ? ConnectSymbols.or : ConnectSymbols.and;
            var innerPrio = cs == CompareSymbol.eq ? (uint)0 : (uint)1;
            var firstParams = values.First();
            var pageSize = firstParams.Count() > 0 ? (int)this.ParameterLimit / firstParams.Count() : (int)this.ParameterLimit;
            var split = this.splitList<IEnumerable<object>>(values, pageSize);

            foreach(var splitValues in split)
            {
                if (values.FirstOrDefault()?.Count() < 2)
                {
                    // single id mode
                    conds.Add(new Condition(cols.First(), cs, splitValues.ToList().Select(v => v.First()).ToArray()));
                }
                else
                {
                    // multiple id mode
                    conds.Add(Condition.Combine(splitValues.Select((lo) => lo.Select((o, k) => new Condition(cols.ElementAt(k), cs, lo.ElementAt(k)) { Priority = innerPrio, ConnectSymbol = innerConnectSymbol })), outerConnectSymbol, innerConnectSymbol));
                }
            }

            return conds;
        }

        public Condition GetConditionForValues(IEnumerable<Column> cols, IEnumerable<IEnumerable<object>> values, CompareSymbol cs = CompareSymbol.eq)
        {
            return Condition.Combine(this.GetConditionsForValues(cols, values, cs));
        }

        #endregion methods
    }
}
