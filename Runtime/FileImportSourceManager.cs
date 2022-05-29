using DatawarehouseCrawler.Model;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.Runtime
{
    public class ImportSourceSettings
    {
        public string AzureStorageConnectionString { get; set; }
        public string LogFileDirectory { get; set; }
        public string SourceFileDirectory { get; set; }
        public bool Resume { get; set; }
        public string[] RunNames { get; set; } 
        public string[] RunGroups { get; set; }
        public string[] ExcludeNames { get; set; }
        public string[] ExcludeGroups { get; set; }
    }

    public class ImportSourceManager
    {
        const string AZUREBLOBSTORAGECONTAINERNAME = "source";
        const string RESUMESTATUSFILE = "resumestatus_{0}.txt";
        const string SOURCEFILE = "source.json";

        public ImportSourceManager(ImportSourceSettings settings)
        {
            this.Settings = settings;

            this.Init();
        }

        private string GetSourceFileJSONString()
        {
            if (!string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString))
            {
                var storageAccount = CloudStorageAccount.Parse(this.Settings.AzureStorageConnectionString);
                var client = storageAccount.CreateCloudBlobClient();
                var container = client.GetContainerReference(AZUREBLOBSTORAGECONTAINERNAME);
                var exist = container.ExistsAsync();
                exist.Wait();
                if (!exist.Result) { throw new ArgumentException($"Azure Storage Blob Container - {AZUREBLOBSTORAGECONTAINERNAME} - does not exist"); }
                // Get a reference to the file share we created previously.
                var blob = container.GetBlockBlobReference(SOURCEFILE);
                var txt = blob.DownloadTextAsync();
                txt.Wait();
                return txt.Result;
            }
            else
            {
                var sourcefile = this.Settings.SourceFileDirectory != null ? Path.Combine(this.Settings.SourceFileDirectory, SOURCEFILE) : SOURCEFILE;
                using (var sr = new StreamReader(sourcefile))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private void Init()
        {
            // init source file by Run Names Files
            var sourcefile = this.Settings.SourceFileDirectory != null ? Path.Combine(this.Settings.SourceFileDirectory, SOURCEFILE) : SOURCEFILE;

           List <ImportModel> sources = JsonConvert.DeserializeObject<ImportModel[]>(this.GetSourceFileJSONString()).OrderByDescending(o => (int)o.TypeEnum).OrderByDescending(q => (int)q.ExpectedSizeEnum)?
                    // filter by names
                    .Where(o => this.Settings.RunNames == null || this.Settings.RunNames.Length < 1 || this.Settings.RunNames.Contains(o.Name.Trim().ToLower()))
                    .Where(o => this.Settings.ExcludeNames == null || this.Settings.ExcludeNames.Length < 1 || !this.Settings.ExcludeNames.Contains(o.Name.Trim().ToLower()))
                    // filter by connection groups
                    .Where(o => this.Settings.RunGroups == null || this.Settings.RunGroups.Length < 1 || this.Settings.RunGroups.Contains(o.Connection.Trim().ToLower()))
                    .Where(o => this.Settings.ExcludeGroups == null || this.Settings.ExcludeGroups.Length < 1 || !this.Settings.ExcludeGroups.Contains(o.Connection.Trim().ToLower()))
                    ?.ToList(); 

            // move items to grouped dictionary
            IEnumerable<string> keys = sources.Select(o => o.Connection.Trim().ToLower()).Distinct();
            Dictionary<string, List<Model.ImportModel>> groupedSources = new Dictionary<string, List<Model.ImportModel>>();
            foreach (var k in keys) groupedSources.Add(k, new List<ImportModel>());
            sources.ForEach(o => groupedSources[o.Connection.Trim().ToLower()].Add(o));

            // generate resumestatusfiles and filter sources by status file
            foreach (var k in keys)
            {
                var resumeFile = string.Format(RESUMESTATUSFILE, k);
                var resumeFilePath = string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString) && !string.IsNullOrEmpty(this.Settings.LogFileDirectory) ? Path.Combine(this.Settings.LogFileDirectory, resumeFile) : resumeFile;
                resumeStatusFilenames.Add(k, resumeFilePath);
                // init resumestates
                if (this.Settings.Resume)
                {
                    string[] resumeStatus = { };
                    // read last status
                    if (!string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString))
                    {
                        resumeStatus = this.ReadLinesFromAzureBlob(resumeFile);
                    }
                    else
                    {
                        if (File.Exists(resumeFilePath)) { resumeStatus = File.ReadAllLines(resumeFilePath)?.ToArray(); }
                    }

                    // filter status list and sort it by expected size (dim before fact then smallest first)
                    groupedSources[k] = groupedSources[k].Where(o => !resumeStatus.Contains(o.Name.ToLower())).ToList();
                }
                else
                {
                    if (!string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString))
                    {
                        this.DeleteAzureBlob(resumeFile);
                    }
                    else
                    {
                        if (File.Exists(resumeFilePath)) File.Delete(resumeFilePath);
                    }
                }
            }

            this.Result = groupedSources;
        }

        private Dictionary<string, string> resumeStatusFilenames = new Dictionary<string, string>();

        public ImportSourceSettings Settings { get; private set; }

        public Dictionary<string, List<Model.ImportModel>> Result { get; private set; }

        private string[] ReadLinesFromAzureBlob(string filename)
        {
            var result = new List<string>();
            var storageAccount = CloudStorageAccount.Parse(this.Settings.AzureStorageConnectionString);
            var client = storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(AZUREBLOBSTORAGECONTAINERNAME);
            var exist = container.ExistsAsync();
            exist.Wait();
            if (!exist.Result) { throw new ArgumentException($"Azure Storage Blob Container - {AZUREBLOBSTORAGECONTAINERNAME} - does not exist"); }
            // Get a reference to the file share we created previously.
            var blob = container.GetAppendBlobReference(filename);
            // exist not working so use try catch here
            try
            {
                var str = blob.OpenReadAsync();
                str.Wait();
                using (var stream = str.Result)
                {
                    using (StreamReader reader = new StreamReader(stream))
                    {
                        while (!reader.EndOfStream)
                        {
                            result.Add(reader.ReadLine());
                        }
                    }
                }
            }
            catch { }

            return result.ToArray();
        }

        private void DeleteAzureBlob(string filename)
        {
            var storageAccount = CloudStorageAccount.Parse(this.Settings.AzureStorageConnectionString);
            var client = storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(AZUREBLOBSTORAGECONTAINERNAME);
            var exist = container.ExistsAsync();
            exist.Wait();
            if (!exist.Result) { throw new ArgumentException($"Azure Storage Blob Container - {AZUREBLOBSTORAGECONTAINERNAME} - does not exist"); }
            // Get a reference to the file share we created previously.
            var blob = container.GetAppendBlobReference(filename);
            var exists = blob.ExistsAsync();
            exist.Wait();
            if (exist.Result)
            {
                var res = blob.DeleteAsync();
            }
        }

        public void WriteResumeStatusFileLine(string grp, string name)
        {
            var filename = this.resumeStatusFilenames[grp.Trim().ToLower()];
            if (!string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString))
            {
                var storageAccount = CloudStorageAccount.Parse(this.Settings.AzureStorageConnectionString);
                var client = storageAccount.CreateCloudBlobClient();

                // Get a reference to the file share we created previously.
                var container = client.GetContainerReference(AZUREBLOBSTORAGECONTAINERNAME);
                var exist = container.ExistsAsync();
                exist.Wait();
                if (!exist.Result) { throw new ArgumentException($"LOG Manager - The azure storage blob container {AZUREBLOBSTORAGECONTAINERNAME} does not exist"); }

                var blob = container.GetAppendBlobReference(filename);
                exist = blob.ExistsAsync();
                exist.Wait();
                if (!exist.Result) { blob.CreateOrReplaceAsync().Wait(); }
                blob.AppendTextAsync($"{name.Trim().ToLower()}\r\n");
            }
            else
            {
                using (var resumeStatusFW = new StreamWriter(new FileStream(filename, FileMode.Append)))
                {
                    resumeStatusFW.WriteLine(name.Trim().ToLower());
                }
            }
        }

        public void CleanUpResumeStatusFiles()
        {
            foreach (var f in resumeStatusFilenames.Values) {
                // Delete resume file when finished successful

                if (!string.IsNullOrEmpty(this.Settings.AzureStorageConnectionString))
                {
                    this.DeleteAzureBlob(f);
                }
                else
                {
                    if (File.Exists(f)) File.Delete(f);
                }
            }
        }
    }
}
