using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.Model.Log;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.DataAdapters;
using DatawarehouseCrawler.QueryAdapters;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DatawarehouseCrawler
{
    public enum ConsistencyEnum
    {
        NotChecked = 0,
        OK = 1,
        CountDiffersButTrue = 2,
        False = 3,
    }

    public class MetricEventArgs
    {
        public string Name;

        public TimeSpan Value;
    }

    public class ResultCount
    {
        public int SourceCount { get; set; }
        public int TargetCount { get; set; }
        public ConsistencyEnum Consistent { get; set; }
        public ConsistencyEnum AdvancedConsistent { get; set; }

        public ResultCount() { }
        public ResultCount(int sourceCount, int targetCount) { this.SourceCount = sourceCount; this.TargetCount = targetCount; }
    }

    public class IdCompareResult
    {
        public List<IEnumerable<object>> targetMissingIds { get; set; }
        public List<IEnumerable<object>> sourceMissingIds { get; set; }

        public IdCompareResult() { this.targetMissingIds = new List<IEnumerable<object>>(); this.sourceMissingIds = new List<IEnumerable<object>>(); }
    }

    public enum ImportOperationMode
    {
        DataCopy = 0,
        GenerateSchema = 1,
        CheckCount = 2,
        AdvancedConsistencyCheck = 3
    }

    public class DataImporter
    {
        protected ITargetDataAdapter TargetAdapter { get; private set; }

        protected ISourceDataAdapter SourceAdapter { get; private set; }

        private IEnumerable<Column> Columns { get; set; }
        private IEnumerable<Column> InsertDateFields { get; set; }
        private IEnumerable<Column> UpdateDateFields { get; set; }

        private DataTable TempSchemaTable { get; set; }

        public DataImporter(Model.ImportModel import, ISourceDataAdapter sourceAdapter, ITargetDataAdapter targetAdapter, ImportOperationMode importOperationMode = ImportOperationMode.DataCopy)
        {
            this.Import = import;
            this.SourceAdapter = sourceAdapter;
            this.TargetAdapter = targetAdapter;
            // important to to make target transfer query joined fields to local references
            this.TargetAdapter.ReadJoinFieldsFromLocalCopy = true;
            this.OperationMode = importOperationMode;
            // get pagesize from source adapter
            if (sourceAdapter.CustomPageSize != null) { this.SetPagesize(sourceAdapter.CustomPageSize.Value); }
        }

        #region properties and events

        public ImportOperationMode OperationMode { get; set; }

        public bool Force { get; set; }

        public ImportModel Import { get; private set; }

        protected int pagesize = 300000;

        protected int conistencyfixpagesize = 1000000;

        public ResultCount CountStatus { get; set; }

        private IQuerySymbols targetQuerySymbols = new SqlQuerySymbols();

        public int GetPagesize()
        {
            return this.pagesize;
        }

        public void SetPagesize(int newPagesize)
        {
            if (newPagesize < 1) throw new ArgumentException("Pagesize should be greater than zero");
            this.pagesize = newPagesize;
        }

        public void SetConsistencyFixPagesize(int newPagesize)
        {
            if (newPagesize < 1) throw new ArgumentException("Pagesize should be greater than zero");
            this.conistencyfixpagesize = newPagesize;
        }

        public event LogModel.LogEventHandler OnLogMessage;

        public delegate void OperationCompletedEventHandler(object o, ImportStatus e);
        public event ProgressEventHandler OnOperationCompleted;

        public delegate void ProgressEventHandler(object o, ImportStatus e);
        public event ProgressEventHandler OnProgress;

        public delegate void CompletedMetricEventHandler(object o, MetricEventArgs e);
        public event CompletedMetricEventHandler OnCompletedMetric;

        #endregion properties and events

        #region methods

        protected Condition GetMaxDatesCondition(IEnumerable<Column> dateFields, bool includeSameDate, string customDateFormat = null)
        {
            // define vars
            var cachedQueriedMaxValues = new Dictionary<int, object>();
            Func<Column, Condition, object> getMaxValue = (col, cond) => {
                var hc = col.GetHashCode();
                if (cachedQueriedMaxValues.ContainsKey(hc)) { return cachedQueriedMaxValues[hc]; }
                else
                {
                    object val = this.TargetAdapter.GetMaxFieldValue(col, cond);
                    if (!string.IsNullOrEmpty(customDateFormat))
                    {
                        val = DateTime.ParseExact(((DateTime)val).ToString(customDateFormat), customDateFormat, System.Globalization.CultureInfo.InvariantCulture);
                    }
                    cachedQueriedMaxValues.Add(hc, val);
                    return cachedQueriedMaxValues[hc];
                }
            };

            // Logic starts here
            Condition first = null, last = null;

            // create an increasing step query   (1 > v) OR (1 = v AND 2 > v) OR (1 = v AND 2 = v AND 3 > v)
            for (var j = 0; j < dateFields.Count(); j++)
            {
                for (var i = 0; i < j + 1; i++)
                {
                    var current = new Condition(dateFields.ElementAt(i), (i < j ? CompareSymbol.eq : includeSameDate && j == dateFields.Count() - 1 && i == j ? CompareSymbol.ge : CompareSymbol.gt), getMaxValue(dateFields.ElementAt(i), last));
                    if (first == null) { first = current; }
                    if (last != null) { last.ConnectCondition = current;  }
                    last = current;
                }

                last.ConnectSymbol = ConnectSymbols.or;
            }

            return first;
        }

        protected void InitColumns()
        {
            Func<string, string> escapeFieldName = s => s.Trim().TrimStart('[').TrimEnd(']');
            var idFields = this.Import.IdFieldName.Split(',').Select(o => escapeFieldName(o));

            // get native table columns
            var dt = this.SourceAdapter.GetSchema();
            var tableCols = new List<Column>();
            var ignoreUpdateColNames = !string.IsNullOrEmpty(this.Import.IgnoreUpdateColumns) ? this.Import.IgnoreUpdateColumns.Split(',').Select(o => escapeFieldName(o)) : new string[] { };

            Action<Column, DataColumn> initType = (c, dc) =>
            {
                c.Type = dc.DataType;
                c.Length = dc.MaxLength;
                c.Nullable = dc.AllowDBNull;
                c.IsIdentity = idFields.Contains(c.Name) ;
            };

            foreach(DataColumn dc in dt.Columns)
            {
                var c = new Column();
                c.Alias = this.SourceAdapter.Table.Alias; 
                c.Name = dc.ColumnName;
                c.InternalName = dc.ColumnName;
                if (ignoreUpdateColNames.Contains(c.InternalName))
                {
                    c.IgnoreOnUpdate = true;
                }

                initType(c, dc);
                tableCols.Add(c);
            }

            var joinCols = new List<Column>();
            // init join fields
            if (this.Import.Join != null && this.Import.Join.Length > 0)
            {
                foreach (var join in this.Import.Join)
                {
                    var joinIdFields = join.IdFieldName.Split(',').Select(o => escapeFieldName(o)).ToArray();
                    var parentJoinCols = join.ParentJoinFieldName.Split(',').Select(o => tableCols.First(c=>c.Name == escapeFieldName(o))).ToArray();

                    // build join condition
                    Func<string, Column, JoinCondition> getJoinCondition = (j, p) => new JoinCondition() { Column = new Column() { Name = j, Alias = join.Name }, JoinColumn = p, ConnectSymbol = ConnectSymbols.and };
                    var i = 0;
                    var joinCondition = getJoinCondition(joinIdFields[i], parentJoinCols[i]);
                    var current = joinCondition;
                   
                    while (i < parentJoinCols.Count())
                    {
                        current.ConnectCondition = getJoinCondition(joinIdFields[i], parentJoinCols[i]);
                        current = current.ConnectCondition as JoinCondition;
                        i++;
                    }
                    var ecij = this.SourceAdapter.GetExtendedColumnTypeInfo(join.SourceName);

                    foreach (var joinColumName in join.SelectFields.Split(',').Select(o=>escapeFieldName(o)))
                    {
                        joinCols.Add(new JoinedColumn() {InternalName = $"{join.Name}_{joinColumName}", Name = joinColumName, Alias = join.Name, Condition = joinCondition, Model = join } );
                    }

                    // set join col orignal column type info
                    foreach (var c in joinCols)
                    {
                        c.OriginalColumnTypeInfo = ecij.FirstOrDefault(o => o.Name == c.Name);
                    }
                }
                // now get schema with all fields
                dt = this.SourceAdapter.GetSchema(tableCols.Concat(joinCols));

                foreach(var joinCol in joinCols)
                {
                    initType(joinCol, dt.Columns[joinCol.InternalName]);
                }
            }

            this.Columns = tableCols.Concat(joinCols);
            this.InsertDateFields = !string.IsNullOrEmpty(this.Import.InsertQueryDateFieldName) ? this.Import.InsertQueryDateFieldName.Split(',').Select(o => this.Columns.First(c => o.Contains('.') ? c.Alias == escapeFieldName(o.Split('.')[0]) && c.InternalName == escapeFieldName(o.Split('.')[1]) : c.InternalName == escapeFieldName(o))) : new Column[] { };
            this.UpdateDateFields = !string.IsNullOrEmpty(this.Import.UpdateQueryDateFieldName) ? this.Import.UpdateQueryDateFieldName.Split(',').Select(o => this.Columns.First(c => o.Contains('.') ? c.Alias == escapeFieldName(o.Split('.')[0]) && c.InternalName == escapeFieldName(o.Split('.')[1]) : c.InternalName == escapeFieldName(o))) : new Column[] { };

            var eci = this.SourceAdapter.GetExtendedColumnTypeInfo();
            foreach (var c in this.Columns)
            {
                c.OriginalColumnTypeInfo = eci.FirstOrDefault(o => o.Name == c.Name);
            }

            // save datatable to temp file because it can be required later
            this.TempSchemaTable = dt;
        }

        protected bool IsTargetTableExisting()
        {
            var exists = this.TargetAdapter.TableExists();
            if (exists && this.Force)
            {
                this.LogWarning($"Force Mode - Dropping EXISTING Target Table - {this.Import?.Name}");
                this.TargetAdapter.DeleteTable();
                this.LogWarning($"Target Table  - {this.Import?.Name} dropped by force mode");
                exists = false;
            }

            return exists;
        }

        protected void ForceModeDelete()
        {
            this.LogWarning($"Applying force mode DELETE All Entries in Target Table {this.Import.Name}");
            var ct = this.TargetAdapter.DeleteData();
            this.LogWarning($"{ct} entries in {this.Import.Name} deleted by force mode");
        }

        protected IEnumerable<SortOrderField> GetDefaultSortOrder(bool desc = false)
        {
            return this.InsertDateFields.Concat(this.Columns.Where(o => o.IsIdentity && this.InsertDateFields.FirstOrDefault(c => c.Name.ToLower() == o.Name.ToLower()) == null)).Select(c => new SortOrderField(c, desc ? SortDirection.Desc : SortDirection.Asc));
        }

        protected IEnumerable<SelectQuery> GetPaginatedSelectQueries(int partialRowCount, Table table, IEnumerable<Column> selectColumns, Condition topBorderFilter = null, Condition bottomBorderFilter = null, int customPageSize = 0)
        {
            var psize = customPageSize > 0 ? customPageSize : this.pagesize;
            var dataQueries = new List<SelectQuery>();
            var pages = partialRowCount / psize;
            var lastPageSize = partialRowCount % psize;
            // add one page if there is a rest
            if (lastPageSize > 0) { pages++; }
            var sortOrderFields = this.GetDefaultSortOrder();
            for (var i = 0; i < pages; i++)
            {
                var from = i * psize;
                // first pages : last page
                var to = (i < pages - 1 || lastPageSize < 1) ? (psize * (i + 1)) : from + lastPageSize;
                var fetchAmount = to - from;

                dataQueries.Add(new SelectQuery(selectColumns, table, Condition.Combine(bottomBorderFilter, topBorderFilter), sortOrderFields) { Range = new Range(from, to) });
            }
            return dataQueries;
        }

        #endregion methods

        #region LOGIC

        public void Run()
        {
            this.InitColumns();
            var watchOuter = System.Diagnostics.Stopwatch.StartNew();
            var watchCorrection = new System.Diagnostics.Stopwatch();
            var watchCorrectionFast = new System.Diagnostics.Stopwatch();
            ResultCount errorCount = new ResultCount();
            try
            {
                var idCols = this.Columns.Where(o => o.IsIdentity);
                //t = this.SourceConnection.BeginTransaction();
                if (this.OperationMode == ImportOperationMode.GenerateSchema)
                {
                    #region Schema

                    this.LogInfo($"Started Generate Schema Mode {this.Import?.Name} - {DateTime.Now.ToString()}", true);
                    try
                    {
                        var exists = this.IsTargetTableExisting();

                        if (!exists)
                        {
                            this.LogInfo($"{this.Import?.Name}: Connected and started copy shema of {this.Import?.Connection} - {this.Import?.SourceName}");

                            var dt = this.TempSchemaTable ?? this.SourceAdapter.GetSchema();
                            exists = this.TargetAdapter.CreateTable(dt, this.Columns.Where(c => c.IsIdentity), this.Columns, this.Import.ExpectedSizeEnum, this.Import.DistributionColumn, this.Import.AzureDwhIgnoreIdentity);
                            if (!exists) throw new Exception("Table was not created");
                            this.OperationCompleted(new ImportStatus() { Name = this.Import.Name, Connection = this.Import.Connection, StatusEnum = StatusEnum.Initialized, TargetCount = 0 });
                            this.LogSuccess($"{this.Import?.Name}: Successfull copied shema of {this.Import?.Connection} - {this.Import?.SourceName}");
                        }
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to copy schema of {this.Import?.Connection} - {this.Import?.SourceName}";
                        this.LogError(msg, ex);
                        this.OperationCompleted(new ImportStatus() { Name = this.Import.Name, Connection = this.Import.Connection, StatusEnum = StatusEnum.Error, ErrorMessage = msg });
                        throw new Exception(msg, ex);
                    }

                    #endregion schema
                }
                else if (this.OperationMode == ImportOperationMode.CheckCount)
                {
                    #region Check Count

                    this.LogInfo($"Started Check Count Mode {this.Import?.Name} - {DateTime.Now.ToString()}", true);
                    int sourceCount = 0, targetCount = 0;

                    try
                    {
                        sourceCount = this.SourceAdapter.GetCount();
                        errorCount.SourceCount = sourceCount;
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to check count of source {this.Import?.Connection} - {this.Import?.SourceName}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }
                    try
                    {
                        targetCount = this.TargetAdapter.GetCount();
                        errorCount.TargetCount = targetCount;
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to check count of target {this.Import?.Connection} - {this.Import?.Name}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }

                    if (sourceCount == targetCount) LogSuccess($"Count - Source:{sourceCount} - Target:{targetCount} - OK");
                    else LogWarning($"Count - Source:{sourceCount} - Target:{targetCount} - Difference");

                    this.CountStatus = new ResultCount(sourceCount, targetCount);

                    #endregion CheckCount
                }
                else if (this.OperationMode == ImportOperationMode.AdvancedConsistencyCheck)
                {
                    #region advanced consistency check

                    this.LogInfo($"Started AdvancedConsistencyCheck Mode {this.Import?.Name} - {DateTime.Now.ToString()}", true);
                    int sourceCount = 0, targetCount = 0;
                    bool hasdblkeys = false;

                    try
                    {
                        sourceCount = this.SourceAdapter.GetCount();
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to check count of source {this.Import?.Connection} - {this.Import?.SourceName}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }

                    try
                    {
                        targetCount = this.TargetAdapter.GetCount();
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to check count of target {this.Import?.Connection} - {this.Import?.Name}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }

                    var check = this.RunAdvancedConsitencyCheckAndCorrection(null, out sourceCount, out targetCount, out hasdblkeys, out int del, out int ins, false);
                   
                    if (hasdblkeys)
                    {
                        LogWarning($"Target has double keys");
                    }
                    else
                    {
                        LogSuccess($"No double keys found");
                    }

                    if (sourceCount == targetCount)
                    {
                        LogSuccess($"Count - Source:{sourceCount} - Target:{targetCount} - OK");
                    }
                    else
                    {
                        LogWarning($"Count - Source:{sourceCount} - Target:{targetCount} - Difference");
                    }

                    this.CountStatus = new ResultCount(sourceCount, targetCount) { AdvancedConsistent = check };

                    #endregion advanced consistency check
                }
                // copy data mode
                else if (this.OperationMode == ImportOperationMode.DataCopy)
                {
                    #region Data Copy Mode Selection

                    this.LogInfo($"Started Data Copy Mode {this.Import?.Name} - {DateTime.Now.ToString()}", true);
                    Condition bottomBorderFilter = null, topBorderFilter = null;
                    if (this.Force || this.Import.DataSyncTypeEnum == DataSyncTypeEnum.none)
                    {
                        // do nothing
                    }
                    else if (this.Import.DataSyncTypeEnum == DataSyncTypeEnum.forcedeleteexisting)
                    {
                        this.LogInfo($"{this.Import?.Name}: Force Delete Existing Mode Selected");
                        this.Force = true;
                    }
                    else if (this.Import.DataSyncTypeEnum == DataSyncTypeEnum.appendbyid || this.Import.DataSyncTypeEnum == DataSyncTypeEnum.appendbyidfirst)
                    {
                        #region appendbyid

                        var firstIdCol = idCols.FirstOrDefault();

                        this.LogInfo($"{this.Import?.Name}: Appendbyid Mode Selected");
                        if (idCols.Count() < 1) throw new ArgumentException("For appendbyid mode the idfield name cannot be empty");
                        else if (this.Import.DataSyncTypeEnum == DataSyncTypeEnum.appendbyid && idCols.Count() > 1) throw new ArgumentException("multiple ids found, appendbyid only works with one id");
                        else if (!firstIdCol.IsSqlNumericType) throw new ArgumentException("id has no integer type, only integer types are supported for appendbyid mode");
                        else
                        {
                            // get the latest id number from target
                            try
                            {
                                var ct = this.TargetAdapter.GetCount();

                                if (ct > 0)
                                {
                                    // get top border
                                    var sourceMaxResult = this.SourceAdapter.GetMaxFieldValue(firstIdCol);
                                    topBorderFilter = new Condition(firstIdCol, CompareSymbol.le, sourceMaxResult);

                                    // get bottom border
                                    object targetMaxResult = this.TargetAdapter.GetMaxFieldValue(firstIdCol, null);
                                    bottomBorderFilter = new Condition(firstIdCol, CompareSymbol.gt, targetMaxResult);

                                    this.LogInfo($"{this.Import?.Name}: AppendById Mode - Got source max id:{sourceMaxResult} target max id:{targetMaxResult} from {this.Import?.Connection} - {this.Import?.SourceName}");
                                }
                            }
                            catch (Exception ex)
                            {
                                var msg = $"AppendById Mode - Failed filters from {this.Import?.SourceName}";
                                this.LogError(msg, ex);
                                throw new Exception(msg, ex);
                            }
                        }

                        #endregion appendbyid
                    }
                    else if (this.Import.DataSyncTypeEnum == DataSyncTypeEnum.appendbydate)
                    {
                        #region appendbydate

                        this.LogInfo($"{this.Import.Name}: Appendbydate Mode Selected");
                        if (idCols?.Count() < 1) throw new ArgumentException("For appendbydate mode the idfield name cannot be empty");
                        if (this.InsertDateFields.Count() < 1) throw new ArgumentException("For appendbydate mode the insertdatefield name cannot be empty");
                        else
                        {
                            // get the latest date from target
                            try
                            {
                                var ct = this.TargetAdapter.GetCount();

                                if (ct > 0)
                                {
                                    var maxDatesCondition = this.GetMaxDatesCondition(this.InsertDateFields, true, this.Import.InsertQueryDateFormat);
                                    // LOG
                                    var maxValuesStr = string.Join(",", maxDatesCondition.GetValuesAsString()); var dateTypesStr = string.Join(",", maxDatesCondition.GetSqlTypesAsString()?.Distinct());
                                    this.LogInfo($"AppendByDate Mode - Got max dates:{maxValuesStr} of types:{dateTypesStr} from {this.Import.Name}");
                                    var idValues = this.TargetAdapter.GetFieldValues(idCols, (Condition)maxDatesCondition.Clone());
                                    // LOG
                                    this.LogInfo($"AppendByDate Mode - Got existing exclude ids to exclude for dates:{this.Import.InsertQueryDateFieldName} from {this.Import.Name}");
                                    var idCondition = Condition.Combine(idValues.Select((vl, i) => vl.Select((v, j) => new Condition(idCols.ElementAt(j), CompareSymbol.ne, v) { ConnectSymbol = ConnectSymbols.or, Priority = 1 })), ConnectSymbols.and, ConnectSymbols.or);

                                    bottomBorderFilter = Condition.Combine(maxDatesCondition, idCondition);
                                }
                            }
                            catch (Exception ex)
                            {
                                var msg = $"AppendByDate Mode - Failed get max date from {this.Import.Name}";
                                this.LogError(msg, ex);
                                throw new Exception(msg, ex);
                            }
                        }
                        #endregion appendbydate
                    }
                    else if (this.Import.DataSyncTypeEnum == DataSyncTypeEnum.appendbydatestrict)
                    {
                        #region appendbydatestrict

                        this.LogInfo($"{this.Import.Name}: AppendbydateStrict Mode Selected");
                        if (this.InsertDateFields.Count() < 1) throw new ArgumentException("For appendbydate mode the insertdatefield name cannot be empty");
                        else
                        {
                            // get the latest date from target
                            try
                            {
                                var ct = this.TargetAdapter.GetCount();

                                if (ct > 0)
                                {
                                    var maxDatesCondition = this.GetMaxDatesCondition(this.InsertDateFields, false, this.Import.InsertQueryDateFormat);
                                    // LOG
                                    var maxValuesStr = string.Join(",", maxDatesCondition.GetValuesAsString()); var dateTypesStr = string.Join(",", maxDatesCondition.GetSqlTypesAsString()?.Distinct());
                                    this.LogInfo($"AppendbydateStrict Mode - Got max dates:{maxValuesStr} of types:{dateTypesStr} from {this.Import.Name}");
                                    bottomBorderFilter = maxDatesCondition;
                                }
                            }
                            catch (Exception ex)
                            {
                                var msg = $"AppendByDateStrict Mode - Failed get max date from {this.Import.Name}";
                                this.LogError(msg, ex);
                                throw new Exception(msg, ex);
                            }
                        }
                        #endregion appendbydatestrict
                    }
                    else if (this.Import.DataSyncTypeEnum == DataSyncTypeEnum.appendbyidexclude)
                    {
                        #region appendbyidexclude
                        this.LogInfo($"{this.Import?.Name}: AppendbyIdExclude Mode Selected");
                        if (idCols.Count() < 1) { throw new ArgumentException("For appendbyidexclude mode the idfield name cannot be empty"); }
                        else
                        {
                            try
                            {
                                var ct = this.TargetAdapter.GetCount();
                                if (ct > 0)
                                {
                                    var idValues = this.TargetAdapter.GetFieldValues(idCols, null);
                                    var idExcludeCondition = this.TargetAdapter.GetConditionForValues(idCols, idValues, CompareSymbol.ne);

                                    this.LogInfo($"AppendByIdExclude Mode - Got existing ids to exclude from {this.Import.Name}");

                                    bottomBorderFilter = idExcludeCondition;
                                }
                            }
                            catch (Exception ex)
                            {
                                var msg = $"AppendByIdExclude Mode - Failed get exclude ids from {this.Import.Name}";
                                this.LogError(msg, ex);
                                throw new Exception(msg, ex);
                            }
                        }
                        #endregion appendbyidexclude
                    }

                    #endregion Data Copy Mode Selection

                    #region Data Copy
                    // run copy operation with filters
                    var totalrows = 0;
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    try
                    {
                        totalrows = this.SourceAdapter.GetCount();
                        this.LogInfo($"{this.Import?.Name}: Got total row count ({totalrows} items) from {this.Import?.Connection} - {this.Import?.SourceName}");
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed get row count from {this.Import?.Connection} - {this.Import?.SourceName}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }

                    if (this.Force)
                    {
                        this.ForceModeDelete();
                    }

                    var partialrows = 0;

                    if (bottomBorderFilter != null)
                    {
                        try
                        {
                            var pc = System.Diagnostics.Stopwatch.StartNew();
                            partialrows = this.SourceAdapter.GetCount(Condition.Combine(bottomBorderFilter, topBorderFilter));
                            pc.Stop();
                            this.LogInfo($"{this.Import?.Name}: Got partial row count by custom where clause ({partialrows} items) from {this.Import?.SourceName}, time elapsed {pc.Elapsed}");
                        }
                        catch (Exception ex)
                        {
                            /// when query limit reached use force mode Delete to ensure table data
                            if (ex.Message == "Internal error: An expression services limit has been reached. Please look for potentially complex expressions in your query, and try to simplify them.")
                            {
                                this.LogWarning("Expression services limit has been reached - Using FORCE DELETE to ensure data");
                                this.ForceModeDelete();
                                partialrows = totalrows;
                                bottomBorderFilter = null;
                            }
                            else
                            {
                                var msg = $"{this.Import?.Name}: Failed get partial row count by custom where clause from {this.Import?.Connection} - {this.Import?.SourceName}";
                                this.LogError(msg, ex);
                                throw new Exception(msg, ex);
                            }
                        }
                    }
                    else
                    {
                        partialrows = totalrows;
                    }

                    try
                    {
                        var dataQueries = this.GetPaginatedSelectQueries(partialrows, this.SourceAdapter.Table, this.Columns, topBorderFilter, bottomBorderFilter);
                        var pages = dataQueries.Count();
                        var lastPageSize = dataQueries.Count() > 0 ? dataQueries.Last()?.Range?.Count ?? 0 : 0;
                        // define async query
                        Func<SelectQuery, int, Task<DataSet>> runQueryPage = async (sq, k) =>
                        {
                            this.LogInfo($"{this.Import?.Name} Page:{k + 1} of {pages}: Retreiving data from source {this.Import?.Connection} - {this.Import?.SourceName}");
                            var qw = System.Diagnostics.Stopwatch.StartNew();
                            var ds = await this.SourceAdapter.RunQueryAsync(sq);
                            qw.Stop();
                            var totalPageRows = ds.Tables[0].Rows.Count;
                            this.LogInfo($"{this.Import?.Name} Page:{k + 1} of {pages}: Successfully retreived {totalPageRows} rows from source, time taken {qw.Elapsed}");
                            return ds;
                        };

                        // define async copy
                        Func<DataSet, int, Task> insertPage = async (ds, k) =>
                        {
                            try
                            {
                                this.LogInfo($"{this.Import?.Name} Page:{k + 1} of {pages}: Connected to target and started copy data of {this.Import?.Connection} - {this.Import?.SourceName}");
                                var tw = System.Diagnostics.Stopwatch.StartNew();
                                await this.TargetAdapter.InsertAsync(ds.Tables[0], this.pagesize, (rowsCopied) =>
                                {
                                    var totalPageRows = ds.Tables[0].Rows.Count;
                                    var doneBefore = k > 1 ? (this.pagesize * (k - 1) + lastPageSize) : k == 1 ? lastPageSize : 0;
                                    var rCopied = doneBefore + rowsCopied;
                                    var percent = Convert.ToInt32(Math.Round(Convert.ToDouble(rCopied) / Convert.ToDouble(partialrows) * 100));
                                    if (percent > 100 || rowsCopied > totalPageRows)
                                    {
                                        this.LogInfo($"Overdoing - Copied {rCopied} rows of {partialrows} and {rowsCopied} page rows of {totalPageRows} total page received rows");
                                    }
                                    else
                                    {
                                        this.LogInfo($"{percent}% - Copied {rCopied} rows of {partialrows}");
                                    }

                                    this.Progress(percent, Convert.ToInt32(rCopied));
                                });

                                tw.Stop();
                                this.LogInfo($"{this.Import?.Name} Page:{k + 1} of {pages}: Successfull copied data of {this.Import?.Connection} - {this.Import?.SourceName} time taken  {tw.Elapsed}");
                            }
                            catch (Exception ex)
                            {
                                var msg = $"{this.Import?.Name} Page:{k + 1} of {pages}: Failed to copy data of {this.Import?.Connection} - {this.Import?.SourceName}";
                                this.LogError(msg, ex);
                                throw new Exception(msg, ex);
                            }
                        };

                        // run pages in shuffled operations to query next source page while writing the current one
                        Task copyOperation = null;
                        foreach (var q in dataQueries.Select((value, i) => new { i, value }))
                        {
                            var queryOperation = runQueryPage(q.value, q.i);
                            copyOperation?.Wait();
                            queryOperation.Wait();
                            copyOperation = insertPage(queryOperation.Result, q.i);
                        }

                        copyOperation?.Wait();
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to copy data, {ex.InnerException?.Message}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex); ;
                    }

                    #endregion Data Copy

                    #region Update
                    int updateRowsCount = 0;
                    if (Import.UpdateModeEnum != UpdateModeEnum.none)
                    {
                        this.LogInfo($"Started Update Mode {this.Import?.Name} - {DateTime.Now.ToString()}", true);
                        //var updatePageSize = 50;

                        //var updateOrderBy = this.GetOrderBySourceQueryFields(true);
                        Condition updateSelectCondition = null;

                        if (Import.UpdateModeEnum == UpdateModeEnum.updateByModifiedDate)
                        {
                            if (this.UpdateDateFields == null || this.UpdateDateFields.Count() < 1) { throw new ArgumentException("UpdateQueryDateFieldName cannot be empty for update by modified date"); }

                            updateSelectCondition = this.GetMaxDatesCondition(this.UpdateDateFields, false, this.Import.UpdateQueryDateFormat);

                            // LOG
                            this.LogInfo($"UpdateByModifiedDate Mode - Got max dates:{string.Join(",", updateSelectCondition.GetValuesAsString())} of types:{string.Join(",", updateSelectCondition.GetSqlTypesAsString())} from {this.Import.Name}");
                        }
                        else if (Import.UpdateModeEnum == UpdateModeEnum.forceupdateall)
                        {
                            updateSelectCondition = null;
                            this.LogInfo($"ForceUpdateAll Mode Selected - query filter set to empty {this.Import.Name}");
                        }

                        var uc = System.Diagnostics.Stopwatch.StartNew();
                        updateRowsCount = this.SourceAdapter.GetCount(updateSelectCondition);
                        uc.Stop();
                        this.LogInfo($"{this.Import?.Name}: Got update row count by custom where clause ({updateRowsCount} items) from {this.Import?.SourceName}, time elapsed {uc.Elapsed}");
                        if (updateRowsCount > 0)
                        {
                            // cache values

                            // apply paging
                            var dataQueries = new List<SelectQuery>();
                            var pages = updateRowsCount / this.pagesize;
                            var lastPageSize = updateRowsCount % this.pagesize;
                            // add one page if there is a rest
                            if (lastPageSize > 0) { pages++; }
                            var sortOrderFields = this.UpdateDateFields.Concat(this.Columns.Where(o => o.IsIdentity)).Select(c => new SortOrderField(c));
                            for (var i = 0; i < pages; i++)
                            {
                                var from = i * pagesize;
                                // first pages : last page
                                var to = (i < pages - 1 || lastPageSize < 1) ? (pagesize * (i + 1)) : from + lastPageSize;
                                var fetchAmount = to - from;

                                dataQueries.Add(new SelectQuery(this.Columns, this.SourceAdapter.Table, updateSelectCondition, sortOrderFields) { Range = new Range(from, to) });
                                //dataQueries.Add(this.GetPageQuery(from, to, fetchAmount, updateOrderBy.FinalQueryOrderByFields, updateFilter));
                            }

                            // define async query
                            Func<SelectQuery, int, Task<DataSet>> runQueryPage = async (sq, k) =>
                            {
                                this.LogInfo($"Update - {this.Import?.Name} Page:{k + 1} of {pages}: Retreiving data from source {this.Import?.Connection} - {this.Import?.SourceName}");
                                var qw = System.Diagnostics.Stopwatch.StartNew();
                                var ds = await this.SourceAdapter.RunQueryAsync(sq);
                                qw.Stop();
                                var totalPageRows = ds.Tables[0].Rows.Count;
                                if (totalPageRows < 1) { throw new Exception("Update query failed - Received No Rows for page"); }
                                this.LogInfo($"Update - {this.Import?.Name} Page:{k + 1} of {pages}: Successfully retreived {totalPageRows} rows from source, time taken {qw.Elapsed}");
                                return ds;
                            };

                            // define async copy
                            Func<DataSet, int, Task> updatePage = async (ds, k) =>
                            {
                                try
                                {
                                    this.LogInfo($"{this.Import?.Name} Page:{k + 1} of {pages}: Connected to target and started update data of {this.Import?.Connection} - {this.Import?.SourceName}");

                                    // prepare queries
                                    var tw = System.Diagnostics.Stopwatch.StartNew();
                                    var updateQueries = new UpdateQuery[ds.Tables[0].Rows.Count];

                                    for (var i = 0; i < updateQueries.Length; i++)
                                    {
                                        var row = ds.Tables[0].Rows[i];
                                        updateQueries[i] = new UpdateQuery(this.TargetAdapter.Table,
                                            this.Columns.Where(c => !c.IsIdentity && !c.IgnoreOnUpdate).Select(c => new UpdateAssign() { Column = c, Value = !row.IsNull(c.InternalName) ? row[c.InternalName] : DBNull.Value }),
                                                Condition.Combine(this.Columns.Where(c => c.IsIdentity).Select(c => new Condition(c, CompareSymbol.eq, row[c.InternalName])))
                                            );
                                    }

                                    var updatedCount = await this.TargetAdapter.RunUpdateAsync(updateQueries);

                                    tw.Stop();
                                    this.LogInfo($"{this.Import?.Name} Page:{k + 1} of {pages}: Successfull updated {updatedCount} data of {this.Import?.Connection} - {this.Import?.SourceName} time taken  {tw.Elapsed}");
                                }
                                catch (Exception ex)
                                {
                                    var msg = $"{this.Import?.Name} Page:{k + 1} of {pages}: Failed to update data of {this.Import?.Connection} - {this.Import?.SourceName}";
                                    this.LogError(msg, ex);
                                    throw new Exception(msg, ex);
                                }
                            };

                            // run pages in shuffled operations to query next source page while writing the current one
                            Task updateOperation = null;

                            foreach (var q in dataQueries.Select((value, i) => new { i, value }))
                            {
                                // run query
                                var queryOperation = runQueryPage(q.value, q.i);
                                updateOperation?.Wait();
                                queryOperation.Wait();
                                updateOperation = updatePage(queryOperation.Result, q.i);
                            }

                            updateOperation?.Wait();
                        }
                    }

                    #endregion Update

                    #region status check 

                    #region apply fix

                    var result = new ResultCount();

                    int targetCount = 0, sourceCount = 0;
                    var targetCountA = this.TargetAdapter.GetCountAsync();
                    var sourceCountA = this.SourceAdapter.GetCountAsync(topBorderFilter);
                    targetCountA.Wait();
                    sourceCountA.Wait();

                    errorCount.TargetCount = targetCount = (int)targetCountA.Result;
                    errorCount.SourceCount = sourceCount = (int)sourceCountA.Result;

                    var hasdblkeys = this.Import.IgnoreKeyIntegrityCheck || this.TargetAdapter.HasDuplicateKeys(idCols);

                    int correctedInsertedRows = 0;
                    int correctedDeletedRows = 0;
                    
                    if (hasdblkeys || targetCount != sourceCount)
                    {
                        if (this.Import.AvoidCompleteConsistencyCorrection)
                        {
                            this.LogWarning($"{this.Import?.Name} - Avoid ConsistencyCorrection Selected - Correction will be scipped now");
                        }
                        else
                        {
                            watchCorrection.Start();
                            this.LogInfo($"{this.Import?.Name} - Started Consistency correction because count differs source:{sourceCount} and target:{targetCount} - {DateTime.Now.ToString()}", true);
                            result.AdvancedConsistent = this.RunAdvancedConsitencyCheckAndCorrection(topBorderFilter, out sourceCount, out targetCount, out hasdblkeys, out correctedInsertedRows, out correctedDeletedRows);
                            if (result.AdvancedConsistent == ConsistencyEnum.OK)
                            {
                                this.LogSuccess($"{this.Import?.Name} - Successfully corrected consistency {this.Import?.Name} - {DateTime.Now.ToString()}");
                            }
                            else if (result.AdvancedConsistent == ConsistencyEnum.CountDiffersButTrue)
                            {
                                this.LogWarning($"{this.Import?.Name} - Corrected consistency but count still differs. Should be corrected on next run {this.Import?.Name} - {DateTime.Now.ToString()}");
                            }
                            watchCorrection.Stop();
                            this.LogInfo($"{this.Import?.Name} - Consistency correction finished - Time taken {watchCorrection.Elapsed.ToString()} - {DateTime.Now.ToString()}", true);
                        }
                    }
                    else
                    {
                        result.AdvancedConsistent = ConsistencyEnum.NotChecked;
                    }

                    #endregion apply fix

                    try
                    {

                        errorCount.TargetCount = result.TargetCount = targetCount;
                        errorCount.SourceCount = result.SourceCount = sourceCount;
                    }
                    catch (Exception ex)
                    {
                        var msg = $"{this.Import?.Name}: Failed to check copied row count {this.Import?.Connection} - {this.Import?.SourceName} to {this.Import?.Name}";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }

                    if (sourceCount != totalrows)
                    {
                        LogWarning($"Warning: Source Count changed during import from before ({totalrows}rows) and now ({sourceCount}rows) - {DateTime.Now.ToString()}");
                    }

                    var keyIntegrityStatus = KeyIntegrityStatusEnum.Idle;

                    try
                    {
                        if (!this.Import.IgnoreKeyIntegrityCheck)
                        {
                            keyIntegrityStatus = hasdblkeys ? KeyIntegrityStatusEnum.DoubleKeys : KeyIntegrityStatusEnum.OK;
                            if (hasdblkeys)
                            {
                                if (result.SourceCount >= result.TargetCount)
                                {
                                    result.Consistent = result.SourceCount > result.TargetCount ? ConsistencyEnum.CountDiffersButTrue : ConsistencyEnum.OK;
                                }
                                else
                                {
                                    result.Consistent = ConsistencyEnum.False;
                                }
                            }
                        }
                        else
                        {
                            result.Consistent = ConsistencyEnum.False;
                            keyIntegrityStatus = KeyIntegrityStatusEnum.Ignored;
                            LogInfo($"{this.Import?.Name}: Key Integrity check skipped due to ignore setting");
                        }
                    }
                    catch (Exception ex)
                    {
                        result.Consistent = ConsistencyEnum.False;
                        keyIntegrityStatus = KeyIntegrityStatusEnum.Error;
                        var msg = $"{this.Import?.Name}: Failed to check key integrity";
                        this.LogError(msg, ex);
                        throw new Exception(msg, ex);
                    }

                    if (sourceCount != totalrows)
                    {
                        LogWarning($"Warning: Source Count changed during import from before ({totalrows}rows) and now ({sourceCount}rows) - {DateTime.Now.ToString()}");
                    }
                    this.CountStatus = result;
                    var importStatus = new ImportStatus()
                    {
                        Name = this.Import.Name,
                        Connection = this.Import.Connection,
                        KeyIntegrityStatusEnum = keyIntegrityStatus,
                        ConsitencyFixStatus = (int)result.AdvancedConsistent,
                        Progress = 100,
                        ProgressedRows = partialrows,
                        RowsInserted = partialrows,
                        RowsUpdated = updateRowsCount,
                        CorrectionRowsInserted = correctedInsertedRows,
                        CorrectionRowsDeleted = correctedDeletedRows,
                        TotalRowsInserted = partialrows + correctedInsertedRows,
                        TotalRowsDifference = partialrows + correctedInsertedRows - correctedDeletedRows,
                        DurationTime = watch.Elapsed,
                        ConsistencyFixDurationTime = watchCorrection.Elapsed,
                        ConsistencyFastFixDurationTime = watchCorrectionFast.Elapsed,
                        SourceCount = sourceCount,
                        TargetCount = targetCount
                    };

                    watch.Stop();
                    bool warning = false;
                    if (!this.Import.IgnoreCountConsistencyCheck && targetCount != sourceCount)
                    {
                        importStatus.StatusEnum = StatusEnum.Warning;
                        this.OperationCompleted(importStatus);
                        warning = true;
                        LogWarning($"Finished with Warning: Row Count difference between source ({sourceCount}rows) and target ({targetCount}rows) - {DateTime.Now.ToString()}");
                    }
                    if (!this.Import.IgnoreKeyIntegrityCheck && hasdblkeys)
                    {
                        importStatus.StatusEnum = StatusEnum.Warning;
                        this.OperationCompleted(importStatus);
                        warning = true;
                        LogWarning($"Finished with Warning: Target has double keys - {DateTime.Now.ToString()}");
                    }
                    if(!warning)
                    {
                        importStatus.StatusEnum = StatusEnum.OK;
                        this.OperationCompleted(importStatus);

                        LogSuccess($"Finished Successfull = integrity checks are OK target row count:{targetCount} - {DateTime.Now.ToString()}");
                    }

                    #endregion status message
                }
            }
            catch (Exception ex)
            {
                this.OperationCompleted(new ImportStatus()
                {
                    Name = this.Import.Name,
                    Connection = this.Import.Connection,
                    StatusEnum = StatusEnum.Error,
                    ProgressedRows = 0,
                    RowsInserted = 0,
                    DurationTime = watchOuter.Elapsed,
                    ConsistencyFixDurationTime = watchCorrection.Elapsed,
                    ConsistencyFastFixDurationTime = watchCorrectionFast.Elapsed,
                    SourceCount = errorCount != null ? errorCount.SourceCount : 0,
                    TargetCount = errorCount != null ? errorCount.TargetCount : 0,
                    ErrorMessage = ex.Message
                });
                throw;
            }
            finally
            {

                watchOuter.Stop();

                if (this.OnCompletedMetric != null)
                {
                    this.OnCompletedMetric(this, new MetricEventArgs { Name = this.Import.Name, Value = watchOuter.Elapsed });
                }

                this.LogInfo($"Time taken, {watchOuter.Elapsed.ToString()}");
            }
        }

        public ConsistencyEnum RunAdvancedConsitencyCheckAndCorrection(Condition sourceTopBorder, out int sourceCount, out int targetCount, out bool hasdblkeys, out int insertedRows, out int deletedRows, bool onlyCheck = false)
        {
            insertedRows = 0; deletedRows = 0;
            var idCols = this.Columns.Where(c => c.IsIdentity).ToArray();

            #region common used functions

            Func<IEnumerable<object>, IEnumerable<object>, bool> areEqual = (a, b) =>
            {
                if (a.Count() != b.Count()) { throw new ArgumentException("The arrays must have equal length"); }
                for (var ii = 0; ii < a.Count(); ii++)
                {
                    if (!a.ElementAt(ii).Equals(b.ElementAt(ii)))
                    {
                        return false;
                    }
                }

                return true;
            };

           

            Func<IEnumerable<IEnumerable<object>>, List<IEnumerable<object>>, bool> checkTgtInsert = (tempInsertIds, finalInsertIds) =>
            {
                var conds = this.TargetAdapter.GetConditionsForValues(idCols, tempInsertIds, CompareSymbol.eq);
                bool doInsert = true;
                foreach (var cond in conds)
                {
                    var tct = this.TargetAdapter.GetCount(cond);
                    if (tct > 0)
                    {
                        doInsert = false;
                        this.LogWarning($"{this.Import?.Name} - Consistency fix - at least one item exists in target - insert will be ignorred for this item insert batch- {DateTime.Now.ToString()}");
                    }
                    else
                    {
                        // todo get this batch values and insert at least them before ignoring the whole page
                    }
                }

                if (doInsert)
                {
                    finalInsertIds.AddRange(tempInsertIds);
                }

                return doInsert;
            };

            Func<int> fixDoubleKeys = () =>
            {
                // found duplicate keys in target
                var doubleKeys = this.TargetAdapter.GetDublicateKeys(idCols);
                this.LogWarning($"{this.Import?.Name} - Consistency Correction - Found {doubleKeys.Count()} double keys - Deleting started - {DateTime.Now.ToString()}");

                var conds = this.TargetAdapter.GetConditionsForValues(idCols, doubleKeys);
                var del = 0;
                // delete from target
                foreach (var cond in conds)
                {
                    var deleted = this.TargetAdapter.Delete(cond);
                    if (deleted < 1) { throw new Exception("Apply Fix - Delete - No ids deleted. This would lead to infinity loop"); }
                    del += deleted;
                    this.LogInfo($"{this.Import?.Name} - Consistency fix - Deleted {deleted} Double keys - {DateTime.Now.ToString()}");
                }

                return del;
            };

            #endregion common used functions
           
            try
            {
                var i = 0;
                var len = 0;

                // first remove double ids
                hasdblkeys = this.TargetAdapter.HasDuplicateKeys(idCols);
                if (!onlyCheck && !this.Import.IgnoreKeyIntegrityCheck && this.TargetAdapter.HasDuplicateKeys(idCols))
                {
                    deletedRows += fixDoubleKeys();
                }

                if (!this.Import.AvoidCountConsistencyCorrection)
                {
                    // now start paging
                    do
                    {
                        var targetCountA1 = this.TargetAdapter.GetCountAsync();
                        var sourceCountA1 = this.SourceAdapter.GetCountAsync(sourceTopBorder);
                        targetCountA1.Wait();
                        sourceCountA1.Wait();

                        targetCount = (int)targetCountA1.Result;
                        sourceCount = (int)sourceCountA1.Result;

                        var targetPageQueries = this.GetPaginatedSelectQueries(targetCount, this.TargetAdapter.Table, idCols, null, null, this.conistencyfixpagesize);
                        var sourcePageQueries = this.GetPaginatedSelectQueries(sourceCount, this.SourceAdapter.Table, idCols, sourceTopBorder, null, this.conistencyfixpagesize);
                        len = new int[] { targetPageQueries.Count(), sourcePageQueries.Count() }.Max();

                        var tpq = targetPageQueries.ElementAt(i);
                        var spq = sourcePageQueries.ElementAt(i);

                        var srcValsA = this.SourceAdapter.GetFieldValuesAsync(spq);
                        var tgtValsA = this.TargetAdapter.GetFieldValuesAsync(tpq);
                        var srcVals = srcValsA.Result.ToList();
                        var tgtVals = tgtValsA.Result.ToList();


                        this.LogInfo($"{this.Import?.Name} - Consistency Correction - Page {(i + 1)} of {len} - Items per page {this.conistencyfixpagesize} - {DateTime.Now.ToString()}");

                        int ti = 0, si = 0;
                        var insertTgt = new List<IEnumerable<object>>();
                        var deleteTgt = new List<IEnumerable<object>>();

                        while (ti < tgtVals.Count && si < srcVals.Count)
                        {
                            if (ti >= tgtVals.Count)
                            {
                                ti = tgtVals.Count - 1;
                            }
                            if (si >= srcVals.Count)
                            {
                                si = srcVals.Count - 1;
                            }
                            var t = tgtVals.ElementAt(ti); // ti < tgtVals.Count ? tgtVals.ElementAt(ti) : null;
                            var s = srcVals.ElementAt(si);  // si < srcVals.Count ? srcVals.ElementAt(si) : null;

                            if (!areEqual(t, s))
                            {
                                if (onlyCheck) { return ConsistencyEnum.False; }
                                bool equalSrcFound = false;
                                var possibleInsertIds = new List<IEnumerable<object>>();
                                // check local cache for entry
                                for (var k = si; k < srcVals.Count(); k++)
                                {
                                    if (areEqual(srcVals.ElementAt(k), t))
                                    {
                                        equalSrcFound = true; ;
                                        si = k;
                                        break;
                                    }
                                    else
                                    {
                                        // insert sql item because its missing
                                        possibleInsertIds.Add(srcVals.ElementAt(k));
                                    }
                                }

                                // check whole src table for entry if not found in cahce
                                if (!equalSrcFound)
                                {
                                    var sct = this.SourceAdapter.GetCount(Condition.Combine(t.Select((o, ii) => new Condition(idCols.ElementAt(ii), CompareSymbol.eq, o) { Priority = 1, ConnectSymbol = ConnectSymbols.and })));
                                    if (sct > 0)
                                    {
                                        checkTgtInsert(possibleInsertIds, insertTgt);
                                        break;
                                    }
                                    else
                                    {
                                        // if element not found in whole list add to delete
                                        deleteTgt.Add(t);
                                        ti++;
                                    }
                                }
                                else
                                {
                                    checkTgtInsert(possibleInsertIds, insertTgt);
                                }
                            }
                            else
                            {
                                si++;
                                ti++;
                            }
                        }

                        if (insertTgt.Count > 0)
                        {
                            var conds = this.TargetAdapter.GetConditionsForValues(idCols, insertTgt);
                            // insert to target

                            foreach (var cond in conds)
                            {
                                var ds = this.SourceAdapter.RunQuery(new SelectQuery(this.Columns, this.SourceAdapter.Table, cond));
                                if (ds.Tables[0].Rows.Count < 1) { throw new Exception("Apply Fix - Insert - No ids found with the query. This would lead to infinity loop"); }
                                this.TargetAdapter.Insert(ds.Tables[0], this.pagesize, null);
                                insertedRows += ds.Tables[0].Rows.Count;
                                this.LogInfo($"{this.Import?.Name} - Consistency fix - Inserted {ds.Tables[0].Rows.Count} missing Rows - {DateTime.Now.ToString()}");
                            }
                        }

                        if (deleteTgt.Count > 0)
                        {
                            var conds = this.TargetAdapter.GetConditionsForValues(idCols, deleteTgt);
                            // delete from target
                            foreach (var cond in conds)
                            {
                                var deleted = this.TargetAdapter.Delete(cond);
                                if (deleted < 1) { throw new Exception("Apply Fix - Delete - No ids deleted. This would lead to infinity loop"); }
                                deletedRows += deleted;
                                this.LogInfo($"{this.Import?.Name} - Consistency fix - Deleted {deleted} Rows - {DateTime.Now.ToString()}");
                            }
                        }

                        // restart check on page if items were modified

                        if (insertTgt.Count < 1 && deleteTgt.Count < 1)
                        {
                            i++;
                        }
                    }
                    while (i < len);
                }

            }
            catch (Exception ex)
            {
                throw new Exception("Error at fixing consistency, Paging level", ex);
            }

            try
            {
                var targetCountA1 = this.TargetAdapter.GetCountAsync();
                var sourceCountA1 = this.SourceAdapter.GetCountAsync(sourceTopBorder);
                targetCountA1.Wait();
                sourceCountA1.Wait();

                targetCount = (int)targetCountA1.Result;
                sourceCount = (int)sourceCountA1.Result;

                hasdblkeys = !this.Import.IgnoreKeyIntegrityCheck && this.TargetAdapter.HasDuplicateKeys(idCols);
 
                int itg = 0;
                if (!this.Import.AvoidCountConsistencyCorrection)
                {
                    // fill top gap if necessary, retry 10 times 
                    while ((hasdblkeys || targetCount != sourceCount) && itg < 10)
                    {
                        var diff = Math.Abs(targetCount - sourceCount);

                        // remove double keys
                        if (hasdblkeys)
                        {
                            deletedRows += fixDoubleKeys();
                        }

                        // delete over targets
                        if (targetCount > sourceCount)
                        {
                            if (onlyCheck) { return ConsistencyEnum.False; }
                            var deleteTgt = this.TargetAdapter.GetFieldValues(new SelectQuery(idCols, this.TargetAdapter.Table, this.GetDefaultSortOrder(true)) { Range = new Range(0, diff) });
                            var conds = this.TargetAdapter.GetConditionsForValues(idCols, deleteTgt);
                            foreach (var cond in conds)
                            {
                                var deleted = this.TargetAdapter.Delete(cond);
                                deletedRows += deleted;
                                if (deleted < 1) { throw new Exception("Apply Fix - Delete - No ids deleted. This would lead to infinity loop"); }
                                this.LogInfo($"{this.Import?.Name} - Consistency after fix - Deleted {deleted} Rows - {DateTime.Now.ToString()}");
                            }
                        }

                        // insert over sources
                        if (targetCount < sourceCount)
                        {
                            if (onlyCheck) { return ConsistencyEnum.CountDiffersButTrue; }
                            var possibleInsertTgt = this.SourceAdapter.GetFieldValues(new SelectQuery(idCols, this.SourceAdapter.Table, this.GetDefaultSortOrder(true)) { Range = new Range(0, diff) });
                            var insertTgt = new List<IEnumerable<object>>();
                            checkTgtInsert(possibleInsertTgt, insertTgt);
                            var conds = this.TargetAdapter.GetConditionsForValues(idCols, insertTgt);
                            // insert to target
                            foreach (var cond in conds)
                            {
                                // insert to target
                                var ds = this.SourceAdapter.RunQuery(new SelectQuery(this.Columns, this.SourceAdapter.Table, cond));
                                if (ds.Tables[0].Rows.Count < 1) { throw new Exception("Apply Fix - Insert - No ids found with the query. This would lead to infinity loop"); }
                                this.TargetAdapter.Insert(ds.Tables[0], this.pagesize, null);
                                insertedRows += ds.Tables[0].Rows.Count;
                                this.LogInfo($"{this.Import?.Name} - Consistency after fix - Inserted {ds.Tables[0].Rows.Count} missing Rows - {DateTime.Now.ToString()}");
                            }
                        }

                        itg++;

                        targetCountA1 = this.TargetAdapter.GetCountAsync();
                        sourceCountA1 = this.SourceAdapter.GetCountAsync(sourceTopBorder);
                        targetCountA1.Wait();
                        sourceCountA1.Wait();

                        targetCount = (int)targetCountA1.Result;
                        sourceCount = (int)sourceCountA1.Result;

                        hasdblkeys = !this.Import.IgnoreKeyIntegrityCheck && this.TargetAdapter.HasDuplicateKeys(idCols);
                    }
                }

                return ConsistencyEnum.OK;
            }
            catch (Exception ex)
            {
                throw new Exception("Error at fixing consistency, After level", ex);
            }
        }

        public bool HasTargetDuplicateKeys()
        {
            return this.TargetAdapter.HasDuplicateKeys(this.Columns.Where(c => c.IsIdentity));
        }

        #endregion LOGIC

        #region log

        protected virtual void Progress(int progress, int rowsProgressed)
        {
            if (this.OnProgress != null)
            {
                this.OnProgress(this, new ImportStatus() { Name = this.Import.Name, Connection = this.Import.Connection, Progress = progress, ProgressedRows = rowsProgressed });
            }
        }

        protected void OperationCompleted(ImportStatus o)
        {
            if (this.OnOperationCompleted != null)
            {
                this.OnOperationCompleted(this, o);
            }
        }

        protected void LogError(string message, Exception ex)
        {
            if (this.OnLogMessage != null) this.OnLogMessage(this, new LogEventArgs($"{message}, {ex?.Message}", LogEventType.Error) { Exception = ex });
        }

        protected void LogWarning(string message)
        {
            if (this.OnLogMessage != null) this.OnLogMessage(this, new LogEventArgs(message, LogEventType.Warning));
        }

        protected void LogSuccess(string message, bool special = false)
        {
            if (this.OnLogMessage != null) this.OnLogMessage(this, new LogEventArgs(message, LogEventType.Success));
        }

        protected void LogInfo(string message, bool special = false)
        {
            if (this.OnLogMessage != null) this.OnLogMessage(this, new LogEventArgs(message, special));
        }

        #endregion log
    }
}
