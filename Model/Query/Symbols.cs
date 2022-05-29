using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DatawarehouseCrawler.Model.Query
{
    public enum CompareSymbol
    {
        eq = 0,
        ne = 1,
        gt = 2,
        ge = 3,
        lt = 4,
        le = 5
    }

    public enum ConnectSymbols
    {
        and = 0,
        or = 1
    }

    public interface IQuerySymbols
    {
        string Eq { get; }
        string Ne { get; }
        string Gt { get; }
        string Lt { get; }
        string Ge { get; }
        string Le { get; }
        string And { get; }
        string Or { get; }
        string DefaultDateFormat { get; }
        string DefaultDateTimeFormat { get; }

        string FormatValueForQuery(SqlDbType type, string value, string customFormat = null);
    }

    public class SqlQuerySymbols : IQuerySymbols
    {
        public string Eq => "=";
        public string Ne => "<>";
        public string Gt => ">";
        public string Lt => "<";
        public string Ge => ">=";
        public string Le => "<=";
        public string And => "AND";
        public string Or => "OR";
        public string DefaultDateFormat => "yyyy-MM-dd";
        public string DefaultDateTimeFormat => "yyyy-MM-dd HH:mm:ss";
        public string FormatValueForQuery(SqlDbType type, string value, string customFormat = null)
        {
            return !string.IsNullOrEmpty(customFormat) ? $"CAST({value} as {type.ToString("f")})" : value;
        }
    }
}
