using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.Model.Query;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.DataAdapters
{
    public interface ITargetDataAdapter:ISourceDataAdapter
    {
        bool ReadJoinFieldsFromLocalCopy { get; set; }

        bool CreateTable(DataTable schema, IEnumerable<Column> idColumns, IEnumerable<Column> allColumns, ExpectedSizeEnum expectedSize, string distributionColumn, bool azureDwhIgnoreIdentity);

        bool TableExists();

        void DeleteTable();

        int DeleteData();

        bool HasDuplicateKeys(IEnumerable<Column> idColumns);

        IEnumerable<IEnumerable<object>> GetDublicateKeys(IEnumerable<Column> idColumns);

        Task<int> RunUpdateAsync(UpdateQuery update);

        Task<int> RunUpdateAsync(IEnumerable<UpdateQuery> update);

        void Insert(DataTable rows, int pageSize, Action<long> statusCallback);

        int Delete(Condition condition);

        Task InsertAsync(DataTable rows, int pageSize, Action<long> statusCallback);

        Condition GetConditionForValues(IEnumerable<Column> cols, IEnumerable<IEnumerable<object>> values, CompareSymbol cs = CompareSymbol.eq);

        IEnumerable<Condition> GetConditionsForValues(IEnumerable<Column> cols, IEnumerable<IEnumerable<object>> values, CompareSymbol cs = CompareSymbol.eq);
    }
}
