using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using DatawarehouseCrawler.Model.Query;

namespace DatawarehouseCrawler.QueryAdapters
{
    public class DataSetQueryAdapter : IQueryAdapter
    {
        public const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

        private readonly Dictionary<CompareSymbol, string> compareSymbolTranslation = new Dictionary<CompareSymbol, string>
        {
            {CompareSymbol.eq, "=" },
            {CompareSymbol.ne, "<>" },
            {CompareSymbol.gt, ">" },
            {CompareSymbol.ge, ">=" },
            {CompareSymbol.lt, "<" },
            {CompareSymbol.le, "<=" }
        };

        private readonly Dictionary<ConnectSymbols, string> connectSymbolTranslation = new Dictionary<ConnectSymbols, string>
        {
            {ConnectSymbols.and, "AND" },
            {ConnectSymbols.or, "OR" }
        };

        public bool ReadJoinFieldsFromLocalCopy { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        private string GetValueString(Column column, object value)
        {
            if (column.IsSqlIntType)
            {
                return value.ToString();
            }
            else if (column.IsSqlDateType)
            {
                var vd = (DateTime)value;
                return $"'{vd.ToString(DateTimeFormat)}'";
            }
            else
            {
                return $"'{value.ToString()}'";
            }
        }

        public string ConvertCondition(Condition condition, int startParameterId = 0, bool ignoreColumnAlias = false, string paramPrefix = null)
        {
            if (condition == null) { return string.Empty; }
            var cl = condition.ToList();
            var ret = new StringBuilder();
            Condition lastCondition = null;
            int openBrackets = 0;
            int i = startParameterId;
            bool forceOpenBracket = false;

            foreach (var c in cl)
            {
                if (forceOpenBracket || (c.ConnectCondition != null && (c.Priority > lastCondition?.Priority || lastCondition == null && c.Priority > 0)))
                {
                    ret.Append("(");
                    openBrackets++;
                    forceOpenBracket = false;
                }

                if (c.Value.GetType().IsArray)
                {
                    var connSym = c.CompareSymbol == CompareSymbol.eq ? ConnectSymbols.or : ConnectSymbols.and;
                    ret.Append(string.Join($" {this.connectSymbolTranslation[connSym]} ", ((object[])c.Value).Select((o, j) => $"[{c.Column.Name}] {this.compareSymbolTranslation[c.CompareSymbol]} {this.GetValueString(c.Column, o)}")));
                }
                else
                {
                    ret.Append($"[{c.Column.Name}]");
                    ret.Append($" {this.compareSymbolTranslation[c.CompareSymbol]} ");
                    if (c.Value?.GetType() == typeof(Column))
                    {
                        var col = ((Column)c.Value);
                        ret.Append($"{this.EscapeColumnForQuery(col)}");
                    }
                    else
                    {
                        ret.Append(this.GetValueString(c.Column, c.Value));
                    }
                }

                // close bracket
                if (openBrackets > 0 && lastCondition != null && (lastCondition.Priority > c.Priority || lastCondition.ConnectSymbol > c.ConnectSymbol))
                {
                    ret.Append(")");
                    // lower priority to place open bracket to next group
                    if (c.ConnectCondition != null && c.ConnectSymbol != c.ConnectCondition.ConnectSymbol && c.Priority == c.ConnectCondition.Priority) { forceOpenBracket = true; }
                    openBrackets--;
                }

                if (c.ConnectCondition != null)
                {
                    ret.Append($" {this.connectSymbolTranslation[c.ConnectSymbol]} ");
                }
                else
                {
                    // close all open brackets
                    for (var k = 0; k < openBrackets; k++)
                    {
                        ret.Append(")");
                    }
                }

                lastCondition = c;
                i++;
            }

            return ret.ToString();
        }

        public string ConvertSelectQuery(SelectQuery query)
        {
            throw new NotImplementedException("Convert Select is not required for datasets");
        }

        public string ConvertSortOrder(IEnumerable<SortOrderField> sortOrder)
        {
            if (sortOrder == null) { return null; }
            return string.Join(",", sortOrder.Select(o => $"{o.Field.InternalName}{(o.SortDirection == SortDirection.Desc ? " DESC" : string.Empty)}"));
        }

        public string EscapeColumnForQuery(Column col)
        {
            return $"[{col?.InternalName}]";
        }

        public string EscapeColumnsForQuery(IEnumerable<Column> columns)
        {
            if (columns == null) { return string.Empty; }
            return string.Join(',', columns.Select(c => this.EscapeColumnForQuery(c)));
        }

    }
}
