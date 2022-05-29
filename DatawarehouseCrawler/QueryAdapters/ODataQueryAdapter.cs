using DatawarehouseCrawler.Model.Query;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;

namespace DatawarehouseCrawler.QueryAdapters
{
    public class ODataQueryAdapter : IQueryAdapter
    {
        public string DefaultQueryParamsSuffix { get; set; }

        public const string EdmDateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fff";
        //private string EdmDateTimeFormat = "yyyy-MM-dd hh:mm:ss";

        private readonly Dictionary<CompareSymbol, string> compareSymbolTranslation = new Dictionary<CompareSymbol, string>
        {
            {CompareSymbol.eq, "eq" },
            {CompareSymbol.ne, "ne" },
            {CompareSymbol.gt, "gt" },
            {CompareSymbol.ge, "ge" },
            {CompareSymbol.lt, "lt" },
            {CompareSymbol.le, "le" }
        };

        private readonly Dictionary<ConnectSymbols, string> connectSymbolTranslation = new Dictionary<ConnectSymbols, string>
        {
            {ConnectSymbols.and, "and" },
            {ConnectSymbols.or, "or" }
        };

        public bool ReadJoinFieldsFromLocalCopy { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public string EscapeColumnForQuery(Column col)
        {
            return col.Name;
        }

        public string EscapeColumnsForQuery(IEnumerable<Column> columns)
        {
            return string.Join(',', columns.Select(c => this.EscapeColumnForQuery(c)));
        }

        protected string AppendGetParameter(string url, string fullparam)
        {
            if (string.IsNullOrEmpty(fullparam)) return url;
            var split = fullparam.Split('=');
            return this.AppendGetParameter(url, split[0], split[1]);
        }

        protected string AppendGetParameter(string url, string name, string value, bool overwrite = false)
        {
            string n = name.Trim(), v = value.Trim();
            if (url.Contains('?'))
            {
                if (url.Contains(n))
                {
                    if (overwrite)
                    {
                        throw new NotImplementedException("overwriting url param is not implemented yet");
                    }
                }
                else
                {
                    return $"{url}&{n}={v}";
                    //return $"{url}&{HttpUtility.UrlEncode(n)}={HttpUtility.UrlEncode(v)}";
                }
            }
            else
            {
                return $"{url}?{n}={v}";
                //return $"{url}?{HttpUtility.UrlEncode(n)}={HttpUtility.UrlEncode(v)}";
            }

            return url;
        }

        public string ConvertSelectQuery(SelectQuery query)
        {
            var ret = string.Empty;
            Action<string> append = str => { ret=this.AppendGetParameter(ret, str); };
            // append paging

            if (query.Range != null)
            {
                append($"$skip={query.Range.From}");
                if (query.Range.Count > 0)
                {
                    append($"$top={query.Range.Count}");
                }
            }

            var filter = this.ConvertCondition(query.Condition);
            // append filters
            append(!string.IsNullOrEmpty(filter) ? $"$filter={filter}" : null);
            // append order by
            append(query.SortOrderFields != null && query.SortOrderFields.Count() > 0 ? $"$orderby={this.ConvertSortOrder(query.SortOrderFields)}" : null);

            if (!string.IsNullOrEmpty(this.DefaultQueryParamsSuffix))
            {
                if (ret.Contains('?'))
                {
                    ret = $"{ret}&{this.DefaultQueryParamsSuffix}";
                }
                else
                {
                    ret = $"{ret}?{this.DefaultQueryParamsSuffix}";
                }
            }
               
            return ret;
        }

        public string ConvertSortOrder(IEnumerable<SortOrderField> sortOrder)
        {
            if (sortOrder == null) { return null; }
            return string.Join(",", sortOrder.Select(o => $"{o.Field.InternalName}{(o.SortDirection == SortDirection.Desc ? " desc" : string.Empty)}"));
        }

        private string GetValueString(Column column, object value)
        {
            if (column.IsSqlIntType)
            {
                return value.ToString();
            }
            else if (column.IsSqlDateType)
            {
                var vd = (DateTime)value;
                return $"datetime'{vd.ToString(EdmDateTimeFormat)}'";
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
                    ret.Append(string.Join($" {this.connectSymbolTranslation[connSym]} ",((object[])c.Value).Select((o, j) => $"{c.Column.Name} {this.compareSymbolTranslation[c.CompareSymbol]} {this.GetValueString(c.Column, o)}")));
                }
                else
                {
                    ret.Append($"{c.Column.Name}");
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
                    for(var k = 0; k < openBrackets; k++)
                    {
                        ret.Append(")");
                    }
                }

                lastCondition = c;
                i++;
            }

            return ret.ToString();
        }
    }
}
