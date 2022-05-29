using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.Providers.DataSetProviders;
using DatawarehouseCrawler.QueryAdapters;

namespace DatawarehouseCrawler.DataAdapters
{

    public class DataSetDataAdapter : ISourceDataAdapter
    {
        public int? CustomPageSize => null;

        public Table Table { get; protected set; }

        private IDataSetProvider dataSetProvider = null;

        private DataSetQueryAdapter dataSetQueryAdapter = new DataSetQueryAdapter();

        private DataSetQueryAdapter DataSetQueryAdapter => dataSetQueryAdapter;

        public DataSetDataAdapter(IDataSetProvider dataSetProvider)
        {
            this.dataSetProvider = dataSetProvider;
            this.Table = new Table(dataSetProvider.TableName);
        }

        #region methods

        public string ExtractConnectionStringProperty(string connectionString, string property, bool mustexist = true)
        {
            var split = connectionString.Split(";");
            foreach (var s in split)
            {
                var isplit = s.Split('=');
                if (isplit[0]?.Trim().ToLower() == property.Trim().ToLower())
                {
                    return isplit[1];
                };
            }
            if (mustexist) { throw new ArgumentException("GetConnectionStringProperty - Could not find property in connection string"); }
            else return null;
        }

        #endregion methods

        public void ReloadAdapter()
        {
            this.dataSetProvider.Init();
        }

        public void Dispose()
        {
            this.dataSetProvider.Dispose();
        }

        public int GetCount(Condition filter = null)
        {
            var dt = this.dataSetProvider.DataSet.Tables[this.Table.Name];
            if (filter == null) { return dt.Rows.Count; }
            var rows = dt.Select(this.dataSetQueryAdapter.ConvertCondition(filter));
            return rows.Length;
        }

        public Task<object> GetCountAsync(Condition filter = null)
        {
            var t = new Task<object>(() => this.GetCount(filter));
            t.Start();
            return t;
        }

        public IEnumerable<IEnumerable<object>> GetFieldValues(SelectQuery selectQuery)
        {
            var ret = new List<IEnumerable<object>>();
            var ds = this.RunQuery(selectQuery);
            foreach (DataRow dr in ds?.Tables[0].Rows)
            {
                var inner = new List<object>();
                foreach (var col in selectQuery.Columns)
                {
                    inner.Add(dr[col.InternalName]);
                }

                ret.Add(inner);
            }

            return ret;
        }

        public IEnumerable<IEnumerable<object>> GetFieldValues(IEnumerable<Column> fields, Condition condition = null)
        {
            var ds = this.dataSetProvider.DataSet;
            var ret = new List<IEnumerable<object>>();
            IEnumerable<DataRow> rows = null;
            if (condition == null) { rows = ds.Tables[this.Table.Name].Select(); }
            var table = this.GetSchema();
            rows = this.dataSetProvider.DataSet.Tables[this.Table.Name].Select(this.dataSetQueryAdapter.ConvertCondition(condition));
            foreach(var r in rows)
            {
                var inner = new List<object>();
                foreach(DataColumn c in ds.Tables[this.Table.Name].Columns)
                {
                    inner.Add(r[c.ColumnName]);
                }

                ret.Add(inner);
            }

            return ret;
        }

        public Task<IEnumerable<IEnumerable<object>>> GetFieldValuesAsync(SelectQuery selectQuery)
        {
            var t = new Task<IEnumerable<IEnumerable<object>>>(() => this.GetFieldValues(selectQuery));
            t.Start();
            return t;
        }


        public object GetMaxFieldValue(Column sourceField, Condition c = null)
        {
            return this.dataSetProvider.DataSet.Tables[this.Table.Name].AsEnumerable().Max(i => i[sourceField.Name]);
        }

        public DataTable GetSchema(IEnumerable<Column> columns = null)
        {
            var dt = this.dataSetProvider.DataSet.Tables[this.Table.Name].Clone();
            dt.Rows.Clear();
            return dt;
        }

        public IEnumerable<ColumnTypeInfo> GetExtendedColumnTypeInfo(string tableName)
        {
            return new List<ColumnTypeInfo>();
        }

        public DataSet RunQuery(SelectQuery query)
        {
            var ds = this.dataSetProvider.DataSet;
            if (query == null) { return ds.Clone(); }
            var table = this.GetSchema();
            var q = this.dataSetQueryAdapter.ConvertCondition(query.Condition);
            // todo add orderby query;
            var orderBy = this.dataSetQueryAdapter.ConvertSortOrder(query.SortOrderFields);
            var rows = ds.Tables[this.Table.Name].Select(q, orderBy)?.AsEnumerable();

            if (rows != null && query.Range != null)
            {
                if (query.Range.From > 0)
                {
                    rows = rows.Skip(query.Range.From);
                }

                if (query.Range.Count > 0)
                {
                    rows = rows.Take(query.Range.Count);
                }
            }

            var t = rows.CopyToDataTable();
            var dsn = new DataSet();
            dsn.Tables.Add(t);
            return dsn;
        }

        public Task<DataSet> RunQueryAsync(SelectQuery query)
        {
            Task<DataSet> t = new Task<DataSet>(() =>{return this.RunQuery(query); });
            t.Start();
            return t;
        }

        public void RunUpdate(Action<DataRow> each, Func<DataRow, bool> filter)
        {
            var rows = this.dataSetProvider.DataSet.Tables[this.Table.Name].AsEnumerable().Where(r => filter(r));
            foreach (var r in rows) { each(r); r.AcceptChanges(); }
        }

        public void RunUpdate(Action<DataRow> each, string filterQuery)
        {
            var rows = this.dataSetProvider.DataSet.Tables[this.Table.Name].Select(filterQuery);
            foreach (var r in rows) { each(r); r.AcceptChanges(); }
        }

        public void DeleteRows(Func<DataRow, bool> filter)
        {
            var rows = this.dataSetProvider.DataSet.Tables[this.Table.Name].AsEnumerable().Where(r => filter(r));
            foreach (var r in rows) { r.Delete(); }
        }

        public void DeleteRows(string query)
        {
            var rows = this.dataSetProvider.DataSet.Tables[this.Table.Name].Select(query);
            foreach (var r in rows) { r.Delete(); }
        }
    }
}
