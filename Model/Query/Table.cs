using System;
using System.Collections.Generic;
using System.Text;

namespace DatawarehouseCrawler.Model.Query
{
    public class Table
    {
        public const string DEFAULT_TABLE_ALIAS =  "main";
        public string Name { get; set; }

        public string Alias { get; set; }

        public Table() { this.Alias = DEFAULT_TABLE_ALIAS; }
        public Table(string name):this() { this.Name = name; }

        public Table(string name, string alias) { this.Name = name; this.Alias = alias; }

    }
}
