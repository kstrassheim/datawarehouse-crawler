using DatawarehouseCrawler.Model.Query;
using System;
using System.Collections.Generic;
using System.Text;

namespace DatawarehouseCrawler.QueryAdapters
{
    public interface IQueryAdapter
    {
        bool ReadJoinFieldsFromLocalCopy { get; set; }

        string EscapeColumnForQuery(Column c);

        string EscapeColumnsForQuery(IEnumerable<Column> columns);

        string ConvertSelectQuery(SelectQuery query);

        string ConvertCondition(Condition condition, int startParameterId, bool ignoreColumnAlias, string paramPrefix);

        string ConvertSortOrder(IEnumerable<SortOrderField> sortOrder);
    }
}
