using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.IO;

namespace DatawarehouseCrawler.Providers.FileStreamProviders
{

    public class AzureStorageFileStreamProvider : IStreamProvider
    {
        protected string ConnectionString { get; set; }

        protected string ContainerName { get; set; }

        protected string FileName { get; set; }

        public Stream GetStream()
        {
            var storageAccount = CloudStorageAccount.Parse(this.ConnectionString);
            var client = storageAccount.CreateCloudBlobClient();

            // Get a reference to the file share we created previously.
            var container = client.GetContainerReference(this.ContainerName);
            var exist = container.ExistsAsync();
            exist.Wait();
            if (!exist.Result) { throw new ArgumentException($"LOG Manager - The azure storage blob container {this.ContainerName} does not exist"); }

            var blob = container.GetBlobReference(this.FileName);
            exist = blob.ExistsAsync();
            exist.Wait();
            if (exist.Result) { 
                MemoryStream ret = new MemoryStream();
                var dl = blob.DownloadToStreamAsync(ret);
                dl.Wait();
                return ret;
            }
            else
            {
                throw new Exception($"AzureStorage {this.ContainerName}/{this.FileName} does not exist");
            }
        }

        public bool StreamExists()
        {
            var storageAccount = CloudStorageAccount.Parse(this.ConnectionString);
            var client = storageAccount.CreateCloudBlobClient();

            // Get a reference to the file share we created previously.
            var container = client.GetContainerReference(this.ContainerName);
            var exist = container.ExistsAsync();
            exist.Wait();
            if (!exist.Result) { throw new ArgumentException($"LOG Manager - The azure storage blob container {this.ContainerName} does not exist"); }

            var blob = container.GetBlobReference(this.FileName);
            exist = blob.ExistsAsync();
            exist.Wait();
            return exist.Result;
        }

        public AzureStorageFileStreamProvider(string connectionString, string containerName, string filename)
        {
            this.ConnectionString = connectionString;
            this.ContainerName = containerName;
            this.FileName = filename;
        }
    }
}
