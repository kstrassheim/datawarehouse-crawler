using DatawarehouseCrawler;
using DatawarehouseCrawler.DataAdapters;
using DatawarehouseCrawler.Model;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.QueryAdapters;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using DatawarehouseCrawler.Providers.FileStreamProviders;
using DatawarehouseCrawler.Providers.DataSetProviders;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace DatawarehouseCrawler.Runtime
{
    public class Importer
    {
        protected ILogger customLog = null;
        public ImporterRuntimeSettings Settings { get; protected set; }

        public Importer(ImporterRuntimeSettings settings, ILogger customLog = null)
        {
            this.customLog = customLog;
            this.Settings = settings;
        }

        public void Run()
        {
            var logSettings = new LogSettings()
            {
                AzureStorageConnectionString = this.Settings.AzureStorageName,
                LogDirectory = this.Settings.LogDirectory,
                ApplicationInsightsKey = this.Settings.ApplicationInsightsKey,
                NoLogToFile = this.Settings.NoLogToFile,
                Silent = this.Settings.Silent
            };

            using (var applog = new LogManager("app", logSettings, this.customLog))
            {
                // start timer
                var watch = System.Diagnostics.Stopwatch.StartNew();
                
                // init key vault
                KeyVaultClient keyVaultClient = null;
                try
                {
                    keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(new AzureServiceTokenProvider().KeyVaultTokenCallback));
                }
                catch(Exception ex)
                {
                    var exn = new Exception("KeyVaultClient Init - Failed", ex);
                    applog.LogException(exn);
                    throw exn;
                }

                try
                {
                    var imsrcset = new ImportSourceSettings()
                    {
                        AzureStorageConnectionString = this.Settings.AzureStorageName,
                        LogFileDirectory = this.Settings.LogDirectory,
                        SourceFileDirectory = this.Settings.SourceDirectory,
                        Resume = this.Settings.Resume,
                        RunNames = this.Settings.Name,
                        RunGroups = this.Settings.Group,
                        ExcludeNames = this.Settings.ExcludeName,
                        ExcludeGroups = this.Settings.ExcludeGroup
                    };

                    #if (DEBUG)
                        // get source.json file from local on debug
                        imsrcset.AzureStorageConnectionString = null;
                    #endif

                    var sqlsrc = new ImportSourceManager(imsrcset);

                    // init status logger to db

                    List<Action> groupProcesses = new List<Action>();

                    foreach (var grp in sqlsrc.Result.Keys)
                    {
                        groupProcesses.Add(() =>
                        {
                            using (var log = new LogManager(grp, logSettings, this.customLog))
                            {
                                using (SqlConnection targetConnection = new SqlConnection(this.Settings.TargetConnectionString), statCon = new SqlConnection(this.Settings.TargetConnectionString))
                                {
                                    string azureAccessToken = null;
                                    try
                                    {
                                        // init auth token provider
                                        var tokenA = new AzureServiceTokenProvider().GetAccessTokenAsync("https://database.windows.net/", this.Settings.TenantId);
                                        tokenA.Wait();
                                        azureAccessToken = tokenA.Result;
                                        
                                    }
                                    catch (Exception ex)
                                    {
                                        var exn = new Exception("Azure DB Token - Failed to get target db auth token", ex);
                                        applog.LogException(exn);
                                        throw exn;
                                    }

                                    if (!string.IsNullOrEmpty(azureAccessToken))
                                    {
                                        targetConnection.AccessToken = azureAccessToken;
                                        statCon.AccessToken = azureAccessToken;
                                    }

                                    try
                                    {
                                        targetConnection.Open();
                                        statCon.Open();

                                        var isl = new ImportStatusLogger(this.Settings.AzureDwh, statCon);

                                        foreach (var s in sqlsrc.Result[grp])
                                        {
                                            // Execute
                                            try
                                            {
                                                //first get current log
                                                var status = isl.GetLog(s.Name, s.Connection);

                                                // check if process is in idle state now and interrupt only after time elapsed
                                                if (status.ProgressStatusEnum == ProgressStatusEnum.Idle)
                                                {
                                                    if (DateTime.Now.AddHours((this.Settings.Interruptafterhours * -1)) > status.ProgressModified)
                                                    {
                                                        var elapsed = new TimeSpan(DateTime.Now.Ticks - status.ProgressModified.Ticks);
                                                        log.LogWarning($"{s.Name} is in state idle since {elapsed} > {this.Settings.Interruptafterhours}h, INTERRUPTING NOW");
                                                    }
                                                    else
                                                    {
                                                        var elapsed = new TimeSpan(DateTime.Now.Ticks - status.ProgressModified.Ticks);
                                                        log.LogWarning($"{s.Name} is in IDLE state since {elapsed} < {this.Settings.Interruptafterhours}h, ENDING NOW to not interrupt");
                                                        continue;
                                                    }
                                                }

                                                ITargetDataAdapter targetDataAdapter = new DataAdapters.SqlDataAdapter(new Table(s.Name), targetConnection, this.Settings.AzureDwh);
                                                ISourceDataAdapter sourceDataAdapter = null;

                                                // get connection string from key vault
                                                var conStrTask = keyVaultClient.GetSecretAsync($"https://{this.Settings.KeyVaultName}.vault.azure.net" , grp);
                                                conStrTask.Wait();
                                                var sourceConnectionString = conStrTask.Result.Value;

                                                if (s.SourceTypeEnum == SourceTypeEnum.sql)
                                                {
                                                    var conn = new SqlConnection(sourceConnectionString);
                                                    // use azure access token if no user id is provided
                                                    if (!sourceConnectionString.ToLower().Contains("user id") && !string.IsNullOrEmpty(azureAccessToken))
                                                    {
                                                        conn.AccessToken = azureAccessToken;
                                                    }

                                                    sourceDataAdapter = new DataAdapters.SqlDataAdapter(new Table(s.SourceName), conn , false, ConnectionModeEnum.OpenCloseDispose, TransactionModeEnum.ReadOnly) { SqlCmdTimeout = 3600 };
                                                }
                                                else if (s.SourceTypeEnum == SourceTypeEnum.odata)
                                                {
                                                    sourceDataAdapter = new ODataAdapter(new Table(s.SourceName), sourceConnectionString, s.QuerySubUrl ?? s.SourceName) { DefaultQueryParamsSuffix = s.DefaultQueryParamsSuffix };
                                                }
                                                else if (s.SourceTypeEnum == SourceTypeEnum.dataset)
                                                {
                                                    Func<string, string> extractSettingValueFromConnectionString = n =>
                                                    {
                                                        var found = sourceConnectionString.Split(';').First(o => o.StartsWith(n));
                                                        sourceConnectionString = sourceConnectionString.Replace(found + ";", string.Empty);
                                                        return found.Split('=')[1];
                                                    };
                                                    var streamtype = (StreamTypeEnum)Enum.Parse(typeof(StreamTypeEnum), extractSettingValueFromConnectionString("StreamType"));
                                                    var filetype = (FileTypeEnum)Enum.Parse(typeof(FileTypeEnum), extractSettingValueFromConnectionString("FileType"));
                                                    IStreamProvider sp = null;
                                                    if (streamtype == StreamTypeEnum.azurestorage) { 
                                                        var containerName = extractSettingValueFromConnectionString("ContainerName");
                                                        var fileName = extractSettingValueFromConnectionString("FileName");
                                                        sp = new AzureStorageFileStreamProvider(sourceConnectionString, containerName, fileName); 
                                                    }
                                                    else if (streamtype == StreamTypeEnum.ftp)
                                                    {
                                                        var url = extractSettingValueFromConnectionString("Url");
                                                        var username = extractSettingValueFromConnectionString("UserName");
                                                        var password = extractSettingValueFromConnectionString("Password");
                                                        sp = new FtpFileStreamProvider(url, username, password);
                                                    }
                                                    else { throw new ArgumentException($"Stream type {streamtype.ToString("f")} not supported"); }

                                                    var streamexist = sp.StreamExists();
                                                    if (!streamexist && s.IgnoreImportIfSourceIsNotAvailable)
                                                    {
                                                        log.LogInfo($"{s.Name} - Skipping import process because source file not available");
                                                        continue;
                                                    }

                                                    IDataSetProvider dsp = null;
                                                    if (filetype == FileTypeEnum.excelxml) {
                                                        dsp = new ExcelXmlDataSetProvider(sp, s.SourceName);
                                                    }
                                                    else if (filetype == FileTypeEnum.csv)
                                                    {
                                                        dsp = new CsvDataSetProvider(sp, s.SourceName);
                                                    }
                                                    else { throw new ArgumentException($"Stream type {filetype.ToString("f")} not supported"); }

                                                    sourceDataAdapter = new DataSetDataAdapter(dsp);
                                                }

                                                if (sourceDataAdapter == null) { throw new ArgumentException($"Cannot find source data adapter for {s?.Name} type {s?.SourceTypeEnum.ToString("f")}"); }

                                                using (sourceDataAdapter)
                                                {
                                                    DataImporter sc = new DataImporter(s, sourceDataAdapter, targetDataAdapter, ImportOperationMode.GenerateSchema);

                                                    if (status.ApplyFunctionEnum == ApplyFunctionEnum.recreatetable)
                                                    {
                                                        log.LogWarning("Received RECREATE TABLE Command via ImportLog Table - Applying force mode to schema creator");
                                                        sc.Force = true;
                                                    }

                                                    // assign custom args
                                                    if (this.Settings.Force || status.ApplyFunctionEnum == ApplyFunctionEnum.recreatetable) sc.Force = true;
                                                    // assign events
                                                    sc.OnLogMessage += log.LogMessageHandler;
                                                    sc.OnProgress += (object o, ImportStatus ist) => isl.UpdateProgress(ist);
                                                    sc.OnOperationCompleted += (object o, ImportStatus ist) => isl.UpdateStatus(ist);

                                                    DataImporter im = new DataImporter(s, sourceDataAdapter, targetDataAdapter);

                                                    if (im == null) { throw new ArgumentException($"Cannot find data import manager for {s?.Name} type {s?.SourceTypeEnum.ToString("f")}"); }

                                                    // assign custom args
                                                    if (this.Settings.CheckCountMode) im.OperationMode = ImportOperationMode.CheckCount;
                                                    if (this.Settings.Force) im.Force = true;

                                                    // custom page size
                                                    int pagesize = 0;
                                                    if (sourceDataAdapter.CustomPageSize != null && sourceDataAdapter.CustomPageSize.Value > 0) { pagesize = sourceDataAdapter.CustomPageSize.Value; }
                                                    if (s.Pagesize > 0) { pagesize = s.Pagesize; }
                                                    if (this.Settings.Pagesize > 0) { pagesize = this.Settings.Pagesize; }
                                                    if (pagesize > 0) { im.SetPagesize(pagesize); }

                                                    // assign events
                                                    im.OnLogMessage += log.LogMessageHandler;
                                                    im.OnProgress += (object o, ImportStatus ist) => isl.UpdateProgress(ist);
                                                    im.OnOperationCompleted += (object o, ImportStatus ist) => { isl.UpdateStatus(ist); sqlsrc.WriteResumeStatusFileLine(grp, s.Name); };
                                                    im.OnCompletedMetric += (object o, MetricEventArgs me) => { applog.LogTimeMetric(me.Name, me.Value); };

                                                    // run schema copy
                                                    sc.Run();
                                                    // reset db command
                                                    if (status.ApplyFunctionEnum == ApplyFunctionEnum.recreatetable) { isl.ResetApplyFunctionValue(status.Name); }

                                                    if (!this.Settings.OnlySchemaMode)
                                                    {
                                                        // apply table data reset when function selected or AutoFixTablesInErrorState and table in error state twice  
                                                        if (status.ApplyFunctionEnum == ApplyFunctionEnum.flushdata || this.Settings.AutoFixTablesInErrorState && status.StatusEnum == StatusEnum.Error && status.LastStatusEnum == StatusEnum.Error)
                                                        {
                                                            log.LogWarning("Received FLUSH DATA Command via ImportLog Table - Applying force mode to data importer");
                                                            im.Force = true;
                                                        }
                                                        // run data copy
                                                        im.Run();

                                                        if (status.ApplyFunctionEnum == ApplyFunctionEnum.flushdata) { isl.ResetApplyFunctionValue(status.Name); }
                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                log.LogException(ex);
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        statCon.Close();
                                        targetConnection.Close();
                                    }
                                }
                            }
                        });
                    }
                    // invoke whether parallel (connection group) or serial
                    if (this.Settings.Parallel)
                    {
                        Parallel.Invoke(groupProcesses.ToArray());
                    }
                    else
                    {
                        foreach (var p in groupProcesses)
                        {
                            p();
                        }
                    }

                    sqlsrc.CleanUpResumeStatusFiles();

                }
                catch (Exception ex)
                {
                    applog.LogException(ex);
                    throw;
                }
                finally
                {
                    watch.Stop();
                    applog.LogTimeMetric("Import", watch.Elapsed);
                    applog.LogInfo($"Finished Time taken at all - {watch.Elapsed}", true);
                }
            }
        }
    }
}
