using System;
using System.Collections.Generic;
using System.Text;

namespace DatawarehouseCrawler.Model.Query
{
    public class Range
    {
        public int From { get; set; }

        public int To { get; set; }

        public int Count { get; set; }

        public Range() { }
        public Range(int from)
        {
            this.From = from;
        }

        public Range(int from, int to) : this(from)
        {
            this.To = to;
            this.Count = To - From;
        }

        public Range(int from, int to, int count = 0): this(from, to)
        {
            this.Count = count;
        }
    }

    public class SelectQuery
    {
        public IEnumerable<Column> Columns { get; set; }

        public Table Table { get; set; }

        public IEnumerable<SortOrderField> SortOrderFields { get; set; }

        public Condition Condition { get; set; }

        public Range Range { get; set; }

        public SelectQuery() { }

        public SelectQuery(Column column, Table table, IEnumerable<SortOrderField> sortOrderFields = null)
        {
            this.Columns = new Column[] {column };
            this.Table = table;
            this.SortOrderFields = sortOrderFields;
        }

        public SelectQuery(IEnumerable<Column> columns, Table table, IEnumerable<SortOrderField> sortOrderFields = null)
        {
            this.Columns = columns;
            this.Table = table;
            this.SortOrderFields = sortOrderFields;
        }

        public SelectQuery(IEnumerable<Column> columns, Table table, Condition condition = null, IEnumerable<SortOrderField> sortOrderFields = null) : this(columns, table, sortOrderFields)
        {
            this.Condition = condition;
        }
    }

    public class UpdateAssign
    {
        public Column Column { get; set; }

        public object Value { get; set; }
    }

    public class UpdateQuery
    {
        public Table Table { get; set; }

        public IEnumerable<UpdateAssign> Assigns { get; set; }

        public Condition Condition { get; set; }

        public UpdateQuery() {  }

        public UpdateQuery(Table table, IEnumerable<UpdateAssign> assigns, Condition condition)
        {
            this.Table = table; this.Assigns = assigns; this.Condition = condition;
        }
    }
}
