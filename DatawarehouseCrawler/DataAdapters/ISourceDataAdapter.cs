using DatawarehouseCrawler.Model.Query;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.DataAdapters
{
    public interface ISourceDataAdapter:IDisposable
    {
        int? CustomPageSize { get; }

        Table Table { get; }

        DataTable GetSchema(IEnumerable<Column> columns = null);

        IEnumerable<ColumnTypeInfo> GetExtendedColumnTypeInfo(string tableName = null);

        DataSet RunQuery(SelectQuery query);

        Task<DataSet> RunQueryAsync(SelectQuery query);

        Task<object> GetCountAsync(Condition filter = null);

        int GetCount(Condition filter = null);

        object GetMaxFieldValue(Column sourceField, Condition c = null);

        IEnumerable<IEnumerable<object>> GetFieldValues(IEnumerable<Column> fields, Condition condition = null);

        IEnumerable<IEnumerable<object>> GetFieldValues(SelectQuery selectQuery);

        Task<IEnumerable<IEnumerable<object>>> GetFieldValuesAsync(SelectQuery selectQuery);
    }
}
