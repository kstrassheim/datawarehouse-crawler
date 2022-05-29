using System;
using System.Collections.Generic;
using System.Text;

namespace DatawarehouseCrawler.Model.Query
{
    public enum SortDirection
    {
        Asc = 0,
        Desc = 1
    }

    public class SortOrderField
    {
        public Column Field { get; set;}

        public SortDirection SortDirection { get; set; }

        public SortOrderField() { }
        public SortOrderField(Column field, SortDirection sortDirection = SortDirection.Asc)
        {
            this.Field = field;
            this.SortDirection = sortDirection;
        }
    }
}
