using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.ApplicationInsights;
using DatawarehouseCrawler;
using Microsoft.ApplicationInsights.Extensibility;
using System.Configuration;
using Microsoft.ApplicationInsights.DataContracts;
using System.Text.RegularExpressions;
using DatawarehouseCrawler.Model;
using System.Data.SqlClient;
using DatawarehouseCrawler.Runtime;

namespace DatawarehouseCrawler.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var argsl = args.ToDictionary<string, string>(o=>o.TrimStart('-').Split(':')[0]?.ToLower());

            foreach(string set in ConfigurationManager.AppSettings)
            {
                if (!argsl.ContainsKey(set.ToLower()))
                {
                    argsl.Add(set.ToLower(), $"-{set.ToLower()}:{ConfigurationManager.AppSettings[set]?.ToString()}");
                }
            }
            
            var connectionStrings = new Dictionary<string, string>();
            foreach(ConnectionStringSettings c in ConfigurationManager.ConnectionStrings) { connectionStrings.Add(c.Name, c.ConnectionString);  }
            // get args
            var settings = new ImporterRuntimeSettings(argsl.Keys.Select(k=>argsl[k]).ToArray(), connectionStrings);
            var import = new Runtime.Importer(settings);
            import.Run();
        }
    }
}
