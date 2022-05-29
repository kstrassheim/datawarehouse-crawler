using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DatawarehouseCrawler.Model.Log;

namespace DatawarehouseCrawler.Runtime
{
    public class LogSettings
    {
        public string AzureStorageConnectionString { get; set; }

        public string ApplicationInsightsKey { get; set; }

        public bool Silent { get; set; }

        public bool NoLogToFile { get; set; }

        public string LogDirectory { get; set; }
    }

    public class LogManager : IDisposable
    {
        const string AZUREFILESTORAGEBLOBCONTAINERNAME = "log";
        const string LOGFILENAME = "IMPORT_LOG_{0}_{1}.txt";

        protected ILogger customLog = null;

        public LogManager(string logGroupName, LogSettings settings, ILogger customLog = null)
        {
            this.LogGroupName = logGroupName;
            this.Settings = settings;
            this.customLog = customLog;
            this.Init();
        }

        private void Init()
        {
            // application insigts init
            var configuration = TelemetryConfiguration.Active; // Reads ApplicationInsights.config file if present
            configuration.InstrumentationKey = this.Settings.ApplicationInsightsKey;
            this.TelemetryClient = new TelemetryClient(configuration);

            // file log init
            var logfilename = string.Format(LOGFILENAME, this.LogGroupName, DateTime.Now.ToString("yyyy-MM-dd"));
            this.LogFilename = !string.IsNullOrEmpty(this.Settings.LogDirectory) ? Path.Combine(this.Settings.LogDirectory, logfilename) : logfilename;

            // init handler
            this.LogMessageHandler = (object o, LogEventArgs e) =>
            {
                if (!this.Settings.Silent)
                {
                    if (e.Type == LogEventType.Info)
                    {
                        this.LogInfo(e.Message, e.Special);
                    }
                    else if (e.Type == LogEventType.Success)
                    {
                        this.LogSuccess(e.Message);
                    }
                    else if (e.Type == LogEventType.Warning)
                    {
                        this.LogWarning(e.Message);
                    }
                    else if (e.Type == LogEventType.Error)
                    {
                        this.LogException(e.Exception);
                    }
                }
            };
        }

        public string LogGroupName { get; private set; }

        public LogSettings Settings { get; private set; }

        private TelemetryClient TelemetryClient { get; set; }

        private string LogFilename { get; set; }

        /// <summary>
        /// Writes a console line in the color of the color parameter
        /// </summary>
        /// <param name="msg">The message</param>
        /// <param name="color">The color</param>
        public void WriteConsoleLog(string msg, ConsoleColor color)
        {
            var old = System.Console.ForegroundColor;
            System.Console.ForegroundColor = color;
            System.Console.WriteLine(msg);
            System.Console.ForegroundColor = old;
        }

        public void WriteFileLog(string line)
        {
            if (!this.Settings.NoLogToFile) {
                if (!string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString))
                {
                    var storageAccount = CloudStorageAccount.Parse(this.Settings.AzureStorageConnectionString);
                    var client = storageAccount.CreateCloudBlobClient();

                    // Get a reference to the file share we created previously.
                    var container = client.GetContainerReference(AZUREFILESTORAGEBLOBCONTAINERNAME);
                    var exist = container.ExistsAsync();
                    exist.Wait();
                    if (!exist.Result ) { throw new ArgumentException($"LOG Manager - The azure storage blob container {AZUREFILESTORAGEBLOBCONTAINERNAME} does not exist"); }

                    var blob = container.GetAppendBlobReference(this.LogFilename);
                    exist = blob.ExistsAsync();
                    exist.Wait();
                    if (!exist.Result) { blob.CreateOrReplaceAsync().Wait(); }
                    blob.AppendTextAsync($"{line}\r\n");
                }
                else
                {
                    using (var logStream = new StreamWriter(new FileStream(this.LogFilename, FileMode.Append))) { logStream.WriteLine(line); }
                }
            }
        }

        public void LogTimeMetric(string name, TimeSpan value)
        {
            this.TelemetryClient.GetMetric(name).TrackValue(value.TotalSeconds);
        }

        public void LogInfo(string message, bool isSpecial = false)
        {
            this.WriteConsoleLog(message, isSpecial ? ConsoleColor.DarkBlue : System.Console.ForegroundColor);
            this.WriteFileLog(message);
            this.TelemetryClient.TrackTrace(message, SeverityLevel.Verbose);
            this.customLog?.LogInformation(message);
        }

        public void LogWarning(string message)
        {
            this.WriteConsoleLog(message, ConsoleColor.Yellow);
            this.WriteFileLog("Warning - " + message);
            this.TelemetryClient.TrackTrace(message, SeverityLevel.Warning);
            this.customLog?.LogWarning(message);
        }

        public void LogSuccess(string message)
        {
            this.WriteConsoleLog(message, ConsoleColor.Green);
            this.WriteFileLog(message);
            this.TelemetryClient.TrackTrace(message, SeverityLevel.Verbose);
            this.customLog?.LogTrace(message);
        }

        public void LogException(Exception ex)
        {
            this.WriteConsoleLog(ex.Message, ConsoleColor.Red);
            this.WriteFileLog("Error - " + ex?.Message);
            this.TelemetryClient.TrackTrace(ex?.Message, SeverityLevel.Error);
            this.TelemetryClient.TrackException(ex);
            this.customLog?.LogError(ex, ex.Message);
        }

        public void Dispose()
        {
            if (this.TelemetryClient != null)
            {
                this.TelemetryClient.Flush();
                // flush is not blocking so wait a bit
                Task.Delay(5000).Wait();
                this.TelemetryClient = null;
            }
        }

        public LogModel.LogEventHandler LogMessageHandler { get; private set; }
    }
}
