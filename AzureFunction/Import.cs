using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using DatawarehouseCrawler.Runtime;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace DatawarehouseCrawler.AzureFunction
{
    public static class Import
    {
        [FunctionName("Import")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            // get env vars and convert to cmd arguments to get settings
            var envargs = Environment.GetEnvironmentVariables();
            var args = new List<string>();
            var connectionStrings = new Dictionary<string, string>();
            foreach(DictionaryEntry e in envargs) {
                if (e.Key.ToString().ToLower().StartsWith("connectionstring:"))
                {
                    var k = e.Key.ToString().Split(':')[1];
                    connectionStrings.Add(k, e.Value.ToString());
                }
                else
                {
                    args.Add($"-{e.Key.ToString().ToLower()}:{e.Value.ToString()}");
                }
            }

            var settings = new ImporterRuntimeSettings(args.ToArray(), connectionStrings);
            var import = new Importer(settings, log);
            import.Run();
        }
    }
}
