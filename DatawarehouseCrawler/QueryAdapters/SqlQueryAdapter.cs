using DatawarehouseCrawler.Model.Query;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace DatawarehouseCrawler.QueryAdapters
{
    public class SqlQueryAdapter : IQueryAdapter
    {
        public const string DefaultParamPrefix = "p";

        public readonly string CUSTOMROWNUMBERCOLNAME = "CustomRowNumber";
        public int SqlVersion { get; set; }
        public bool ForceOldSqlVersion { get; set; }
        public bool ReadJoinFieldsFromLocalCopy { get; set; }

        public bool UseOldSqlQuery(SelectQuery query)
        {
            return this.ForceOldSqlVersion && query.Range != null || this.SqlVersion != 0 && this.SqlVersion < 11 && query.Range != null;
        }

        private readonly Dictionary<CompareSymbol, string> compareSymbolTranslation = new Dictionary<CompareSymbol, string>
        {
            {CompareSymbol.eq, "=" },
            {CompareSymbol.ne, "<>" },
            {CompareSymbol.gt, ">" },
            {CompareSymbol.ge, ">=" },
            {CompareSymbol.lt, " < " },
            {CompareSymbol.le, "<=" }
        };

        private readonly Dictionary<ConnectSymbols, string> connectSymbolTranslation = new Dictionary<ConnectSymbols, string>
        {
            {ConnectSymbols.and, "AND" },
            {ConnectSymbols.or, "OR" }
        };

        //public string EscapeColumnForQuery(Column col)
        //{
        //    return $"[{col.Alias}].[{col.Name}]{(!string.IsNullOrEmpty(col.Alias) ? $" {col.Alias}" : string.Empty)}:";
        //}

        //public string EscapeColumnsForQuery(IEnumerable<Column> columns)
        //{
        //    return string.Join(',', columns.Select(c => this.EscapeColumnForQuery(c)));
        //}

        public string EscapeColumnForQuery(Column c)
        {
            if (c.GetType() == typeof(JoinedMaxValueColumn))
            {
                return this.ReadJoinFieldsFromLocalCopy ? $"MAX([{c.InternalName}])" : $"MAX([{c.Alias}].[{c.Name}])";
            }
            else if (c.GetType() == typeof(MaxValueColumn))
            {
                return $"MAX([{c.Alias}].[{c.Name}])";
            }
            else if (c.GetType() == typeof(CountRowsColumn))
            {
                return $"COUNT(*)";
            }
            else if (c.GetType() == typeof(JoinedColumn))
            {
                return this.ReadJoinFieldsFromLocalCopy ? $"[{c.InternalName}]" : $"[{c.Alias}].[{c.Name}]";
            }
            else
            {
                return $"[{c.Alias}].[{c.Name}]";
            }
        }

        public string EscapeColumnsForQuery(IEnumerable<Column> cols)
        {
            return string.Join(',', cols.Select(c => $"{this.EscapeColumnForQuery(c)}{(c.Name != c.InternalName ? $" {c.InternalName}":string.Empty)}"));
        }

        public string ConvertCondition(Condition condition, int startParameterId = 0, bool ignoreColumnAlias = false, string paramPrefix = DefaultParamPrefix)
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

                if (this.ReadJoinFieldsFromLocalCopy && c.Column.GetType() == typeof(JoinedColumn))
                {
                    ret.Append($"[{c.Column.InternalName}]");
                }
                else
                {
                    ret.Append($"{(!ignoreColumnAlias ? $"[{c.Column.Alias}]." : string.Empty)}[{c.Column.Name}]");
                }
                
                if (c.Value.GetType().IsArray)
                {
                    ret.Append($" {(c.CompareSymbol == CompareSymbol.ne ? " NOT" : string.Empty)} IN ({string.Join(',', ((object[])c.Value).Select((o,j)=>$"@{paramPrefix}_{i}_{j}"))})");
                }
                else
                {
                    ret.Append(this.compareSymbolTranslation[c.CompareSymbol]);
                    if (c.Value?.GetType() == typeof(Column))
                    {
                        var col = ((Column)c.Value);
                        ret.Append($"{this.EscapeColumnForQuery(col)}");
                    }
                    else 
                    {
                        ret.Append($"@{paramPrefix}_{i}");
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

        public string GetTableJoinQuery(IEnumerable<Column> columns)
        {
            if (columns == null) { return string.Empty; }
            var ret = new StringBuilder();
            var joins = columns?.Where(c => c.GetType() == typeof(JoinedColumn))?.GroupBy(c => ((JoinedColumn)c).Model.SourceName);

            foreach(var grp in joins)
            {
                var joinCol = ((JoinedColumn)grp.First());
                
                ret.Append($" LEFT JOIN {string.Join('.', joinCol.Model.SourceName.Split('.').Select(o => $"[{o.Trim().TrimStart('[').TrimEnd(']')}]"))} {joinCol.Model.Name} ON {this.ConvertCondition(joinCol.Condition)}");
            }

            return ret.ToString();
        }

        public string GetMaxValueQuery(Column col, Table t, Condition con = null)
        {
            var selectColumns = $"{this.EscapeColumnForQuery(col)}";
            var fromCondition = $" FROM {(t.Name.Contains('.') ? string.Join('.', t.Name.Split('.').Select(o => $"[{o.Trim().TrimStart('[').TrimEnd(']')}]")) : t.Name)} {t.Alias}{(!this.ReadJoinFieldsFromLocalCopy ? this.GetTableJoinQuery(new Column[] {col}) : string.Empty)}";
            var whereCondition = con != null ? $" WHERE {this.ConvertCondition(con)}" : string.Empty;
            return $"SELECT {selectColumns}{fromCondition}{whereCondition}";
        }

        public string ConvertSelectQuery(SelectQuery query)
        {
            var selectColumns = this.EscapeColumnsForQuery(query.Columns);
            var tableJoin = !this.ReadJoinFieldsFromLocalCopy ? this.GetTableJoinQuery(query.Columns.Concat(query.Condition?.ToList().Select(c => c.Column) ?? new Column[] { })) : string.Empty;
            var fromCondition = $" FROM {(query.Table.Name.Contains('.') ? string.Join('.', query.Table.Name.Split('.').Select(o => $"[{o.Trim().TrimStart('[').TrimEnd(']')}]")) : query.Table.Name)} {query.Table.Alias}{tableJoin}";
            var whereCondition = query.Condition != null ? $" WHERE {this.ConvertCondition(query.Condition)}" : string.Empty;
            var orderBy = query.SortOrderFields != null && query.SortOrderFields.Count() > 0 ? $" ORDER BY {this.ConvertSortOrder(query.SortOrderFields)}" : string.Empty;
            // use slow query as default for older versions
            if (this.UseOldSqlQuery(query))
            {
                var whereClauses = new List<string>();
                if (query.Range != null)
                {
                    if (query.Range.From > 0)
                    {
                        whereClauses.Add($"q.{CUSTOMROWNUMBERCOLNAME} > {query.Range.From}");
                    }
                    if (query.Range.To > 0)
                    {
                        whereClauses.Add($"q.{CUSTOMROWNUMBERCOLNAME} <= {query.Range.To}");
                    }
                }

                return $"SELECT * FROM (SELECT {selectColumns}, ROW_NUMBER() OVER ({orderBy}) AS {CUSTOMROWNUMBERCOLNAME}{fromCondition}{whereCondition}) q {(whereClauses.Count > 0 ? $" WHERE {string.Join(" AND ", whereClauses)}" : string.Empty)}";

            }
            else
            {
                var ret = new StringBuilder("SELECT ");
                // column selection
                ret.Append(selectColumns);
                // table joins
                ret.Append(fromCondition);

                // where clause
                ret.Append(whereCondition);

                // order by clause
                ret.Append(orderBy);

                // append paging
                ret.Append(query.Range != null ? $" OFFSET { query.Range.From} ROWS {(query.Range.Count > 0 ? $"FETCH NEXT { query.Range.Count} ROWS ONLY" : string.Empty)}" : string.Empty);

                return ret.ToString();
            }
        }

        public string ConvertSortOrder(IEnumerable<SortOrderField> sortOrder)
        {
            if (sortOrder == null) { return null; }
            return string.Join(",", sortOrder.Select(o => $"[{o.Field.Alias}].[{o.Field.Name}]{(o.SortDirection == SortDirection.Desc ? " DESC" : string.Empty)}"));
        }

        public string ConvertUpdateQuery(UpdateQuery query, string paramPrefix = DefaultParamPrefix)
        {
            if (query==null || query.Assigns == null || query.Assigns.Count() < 1) { return null; }
            var ret = new StringBuilder($"UPDATE {(query.Table.Name.Contains('.') ? string.Join('.', query.Table.Name.Split('.').Select(o => $"[{ o }]")) : query.Table.Name)} SET ");

            ret.Append(string.Join(',', query.Assigns.Select((o,i) => $"[{o.Column.InternalName}]=@{(!string.IsNullOrEmpty(paramPrefix) ? $"{paramPrefix}_{i}" :i.ToString())}")));
            ret.Append($" WHERE {this.ConvertCondition(query.Condition, query.Assigns.Count(), true, paramPrefix)}");

            return ret.ToString();
        }

        public IEnumerable<SqlParameter> GetParametersForUpdateQuery(UpdateQuery query, string paramPrefix = DefaultParamPrefix)
        {
            if (query == null || query.Assigns == null || query.Assigns.Count() < 1) { return null; }
            var assignPars = this.GetParametersForUpdateAssigns(query.Assigns, paramPrefix);
            return assignPars.Concat(this.GetParametersForCondition(query.Condition, assignPars.Count(), paramPrefix));
        }

        public IEnumerable<SqlParameter> GetParametersForUpdateAssigns(IEnumerable<UpdateAssign> assigns, string paramPrefix = DefaultParamPrefix)
        {
            if (assigns == null) { return null; }
            var ret = new List<SqlParameter>();
            int i = 0;
            Action<SqlParameter, Column, object> addValueToParameter = (p, c, v) =>
            {
                if (c.Length > 0) {  p.Size = c.Length;  }
                p.IsNullable = c.Nullable;
                p.Value = v;
            };

            foreach (var a in assigns)
            {
                var p = new SqlParameter($"@{paramPrefix}_{i}", a.Column.SqlType);
                // add original column info if available
                if (a.Column.OriginalColumnTypeInfo != null && a.Column.OriginalColumnTypeInfo.Precision != null)
                {
                    p.Precision = Convert.ToByte(a.Column.OriginalColumnTypeInfo.Precision.Value);
                }
                if (a.Column.OriginalColumnTypeInfo != null && a.Column.OriginalColumnTypeInfo.Scale != null)
                {
                    p.Scale = Convert.ToByte(a.Column.OriginalColumnTypeInfo.Scale.Value);
                }

                addValueToParameter(p, a.Column, a.Value);
                ret.Add(p);
                i++;
            }

            return ret;
        }

        public IEnumerable<SqlParameter> GetParametersForCondition(Condition condition, int startFrom = 0, string paramPrefix = DefaultParamPrefix)
        {
            if (condition == null) { return null; }
            var ret = new List<SqlParameter>();
            var cl = condition.ToList();
            var i = startFrom;
            Action<SqlParameter, Column, object> addValueToParameter = (p, c, v) =>
            {
                if (c.Length > 0) { p.Size = c.Length; }
                p.IsNullable = c.Nullable;
                p.Value = v;
            };

            foreach (var c in cl)
            {
                if (c.Value.GetType().IsArray)
                {
                    var valArr = (object[])c.Value;
                    var j = 0;
                    foreach(object val in valArr)
                    {
                        var pl = new SqlParameter($"@{paramPrefix}_{i}_{j}", c.Column.SqlType);
                        // add original column info if available
                        if (c.Column.OriginalColumnTypeInfo != null && c.Column.OriginalColumnTypeInfo.Precision != null)
                        {
                            pl.Precision = Convert.ToByte(c.Column.OriginalColumnTypeInfo.Precision.Value);
                        }
                        if (c.Column.OriginalColumnTypeInfo != null && c.Column.OriginalColumnTypeInfo.Scale != null)
                        {
                            pl.Scale = Convert.ToByte(c.Column.OriginalColumnTypeInfo.Scale.Value);
                        }

                        addValueToParameter(pl, c.Column, val);
                        ret.Add(pl);
                        j++;
                    }

                }
                else
                {
                    if (c.Value?.GetType() != typeof(Column))
                    {
                        var p = new SqlParameter($"@{paramPrefix}_{i}", c.Column.SqlType);
                        addValueToParameter(p, c.Column, c.Value);
                        ret.Add(p);
                    }
                }
               
                i++;
            }

            return ret;
        }
    }
}
