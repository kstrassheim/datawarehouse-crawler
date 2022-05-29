using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DatawarehouseCrawler.Runtime
{
    public class ImporterRuntimeSettings
    {
        protected string[] Args { get; set; }
        public Dictionary<string, string> ConnectionStrings { get; protected set; }
        public string[] Name { get; protected set; }
        public string[] Group { get; protected set; }
        public string[] ExcludeName { get; protected set; }
        public string[] ExcludeGroup { get; protected set; }
        public bool Force { get; protected set; }
        public bool Silent { get; protected set; }
        public bool OnlySchemaMode { get; protected set; }
        public bool CheckCountMode { get; protected set; }
        public bool Resume { get; protected set; }
        public string ApplicationInsightsKey { get; protected set; }
        public string LogDirectory { get; protected set; }
        public bool NoLogToFile { get; protected set; }
        public string SourceDirectory { get; protected set; }
        public bool Parallel { get; protected set; }
        public bool AzureDwh { get; protected set; }
        public int Pagesize { get; protected set; }
        public int ConsistencyfixPagesize { get; protected set; }
        public bool AvoidConsistencyCorrection { get; protected set; }
        public int Interruptafterhours { get; protected set; }
        public string TargetConnectionString { get; protected set; }
        public string AzureStorageName { get; protected set; }
        public string KeyVaultName { get; protected set; }
        public string ClientId { get; protected set; }
        public string ClientSecret { get; protected set; }
        public string TenantId { get; protected set; }
        public bool AutoFixTablesInErrorState { get; protected set; }

        public ImporterRuntimeSettings(string[] args, Dictionary<string, string> connectionStrings) {

            this.Args = args;
            this.ConnectionStrings = connectionStrings;
            this.Name = this.GetStringArrayFromSettings("name"); 
            this.Group = this.GetStringArrayFromSettings("group"); 
            this.ExcludeName = this.GetStringArrayFromSettings("excludename"); 
            this.ExcludeGroup = this.GetStringArrayFromSettings("excludegroup"); 
            this.Force = this.IsSettingsExisting("force"); 
            this.Silent = this.IsSettingsExisting("silent");
            this.OnlySchemaMode= this.IsSettingsExisting("onlyschemamode"); 
            this.CheckCountMode = this.IsSettingsExisting("checkcountmode"); 
            this.Resume = this.IsSettingsExisting("resume");
            this.ApplicationInsightsKey = this.GetStringFromSettings("APPINSIGHTS_INSTRUMENTATIONKEY");

            this.LogDirectory = this.GetStringFromSettings("logdirectory"); 
            this.NoLogToFile = this.IsSettingsExisting("nologtofile");
            this.SourceDirectory = this.GetStringFromSettings("sourcefiledirectory");
            this.Parallel = this.IsSettingsExisting("parallel"); 
            this.AzureDwh = this.IsSettingsExisting("azuredwh");
            this.Pagesize = int.Parse(this.GetStringFromSettings("pagesize", "0"));
            this.ConsistencyfixPagesize = int.Parse(this.GetStringFromSettings("consistencyfixpagesize", "0"));
            this.AvoidConsistencyCorrection = this.IsSettingsExisting("avoidconsistencycorrection");
            this.Interruptafterhours = int.Parse(this.GetStringFromSettings("interruptafterhours", "0"));
            this.AzureStorageName = this.GetStringFromSettings("azurestoragename");
            this.KeyVaultName = this.GetStringFromSettings("keyvaultname");
            this.ClientId = this.GetStringFromSettings("clientid");
            this.ClientSecret = this.GetStringFromSettings("clientsecret");
            this.TenantId = this.GetStringFromSettings("tenantid");
            this.TargetConnectionString = this.GetConnectionStringFromSettings("targetconnectionstring", "Target");
            this.AutoFixTablesInErrorState = this.IsSettingsExisting("autofixtablesinerrorstate");
        }
  
        protected string GetStringFromSettings(string settingName, string defaultValue = null)
        {
            var s = this.Args?.FirstOrDefault(o => o.ToLower().StartsWith($"-{settingName.ToLower()}"));
            if (!string.IsNullOrEmpty(s))
            {
                return s.Contains(':') ? s.Substring(s.Split(':')[0].Length + 1) : true.ToString();
            }
            else
            {
                return defaultValue;
            }
        }

        protected string GetConnectionStringFromSettings(string settingName, string connectionStringName)
        {
            return this.Args?.FirstOrDefault(o => o.ToLower().StartsWith($"-{settingName.ToLower()}:"))?.Split(':')[1] ?? this.ConnectionStrings[connectionStringName]?.ToString();
        }

        protected bool IsSettingsExisting(string settingName)
        {
            return !string.IsNullOrEmpty(this.GetStringFromSettings(settingName));
        }

        protected string[] GetStringArrayFromSettings(string settingName)
        {
            var s = this.GetStringFromSettings(settingName);
            return !string.IsNullOrEmpty(s) ? s.Split(',')?.Select(o => o.Trim().ToLower())?.ToArray() : null;
        }
    }
}
