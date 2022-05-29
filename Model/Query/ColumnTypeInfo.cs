using System;
using System.Collections.Generic;
using System.Text;

namespace DatawarehouseCrawler.Model.Query
{
    public class ColumnTypeInfo
    {
        public string Name { get; set; }

        public string TypeName { get; set; }

        public int? Precision { get; set; }

        public int? Scale { get; set; }
    }
}
