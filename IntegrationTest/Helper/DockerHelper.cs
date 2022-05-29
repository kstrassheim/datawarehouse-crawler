using Docker.DotNet;
using Docker.DotNet.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.IntegrationTest.Helper
{
    public class DockerHelper
    {
        private DockerClient client;

        public string ImageName { get; protected set; }

        public string ContainerName { get; protected set; }

        public string DockerfileTarPackagePath { get; protected set; }

        public IList<string> EnvArgs { get; set; }

        public AuthConfig DockerHubAuthOptional { get; set; }

        public string ImageFilePath { get; set; }

        public int PublicPort { get; protected set; }

        public int PrivatePort { get; protected set; }

        public bool KeepOpen { get; protected set; }

        public Uri DockerUri
        {
            get
            {
                return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? new Uri("npipe://./pipe/docker_engine")
                : new Uri("unix:/var/run/docker.sock");
            }
        }

        public DockerHelper(string imageName, string containerName, int publicPort, int privatePort, string dockerfileTarPackagePath = null)
        {
            this.ImageName = imageName;
            this.ContainerName = containerName;
            this.PublicPort = publicPort;
            this.PrivatePort = privatePort;
            this.DockerfileTarPackagePath = !string.IsNullOrEmpty(dockerfileTarPackagePath) ? Path.GetFullPath(dockerfileTarPackagePath) : null;
            this.client = new DockerClientConfiguration(this.DockerUri).CreateClient();
        }

        protected async Task<CreateContainerResponse> CreateContainerAsync()
        {
            return await CreateContainerAsync(this.client, this.ImageName, this.ContainerName, this.PublicPort, this.PrivatePort, this.EnvArgs);
        }

        protected async Task RemoveContainerAsync()
        {
            await RemoveContainerByNameAsync(this.client, this.ContainerName);
        }

        protected async Task StartContainerAsync()
        {
            await StartContainerAsync(this.client, this.ContainerName, this.ImageName);
        }

        protected async Task EnsureContainerAsync()
        {
            await RemoveContainerAsync();
            await this.CreateContainerAsync();
            await this.StartContainerAsync();
        }

        protected async Task EnsureImageBuildAsync()
        {
            if (!await ImageExistsAsync(this.client, this.ImageName))
            {
                await BuildImageAsync();
            }
        }

        protected async Task BuildImageAsync()
        {
            if (string.IsNullOrEmpty(this.DockerfileTarPackagePath)) throw new ArgumentException("No dockerfile Tar Package path specified");
            await BuildImageAsync(this.client, this.DockerfileTarPackagePath, this.ImageName);
        }

        protected async Task PullImageAsync()
        {
            await PullImageAsync(this.client, this.ImageName, this.DockerHubAuthOptional);
        }

        protected async Task LoadImageFromFileAsync()
        {
            await LoadImageFromFileAsync(this.client, this.ImageFilePath);
        }

        public async Task<bool> IsStarted()
        {
            return await IsContainerStartedAsync(this.client, this.ContainerName);
        }

        public async Task Start()
        {
            //if (!string.IsNullOrEmpty(this.DockerfileTarPackagePath))
            //{
            //    await this.EnsureImageBuildAsync();
            //}
            //else if(!string.IsNullOrEmpty(this.ImageFilePath))
            //{

            //    await this.LoadImageFromFileAsync();
            //}
            //else
            //{
            //    await this.PullImageAsync();
            //}
            // await this.EnsureContainerAsync();
        }

        public async Task Stop()
        {
            await this.RemoveContainerAsync();
        }

        #region static methods

        public static async Task<ContainerListResponse> GetContainerByNameAsync(DockerClient client, string containerName)
        {
            try
            {
                var all = await client.Containers.ListContainersAsync(new ContainersListParameters() { All = true });
                var existing = all?.FirstOrDefault(o => o.Names?.FirstOrDefault(n => n.ToLower() == $"/{containerName}") != null);
                return existing;
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to get container {containerName} by name";
                throw new Exception(msg, ex);
            }
        }
        public static async Task<ImagesListResponse> GetImageByNameAsync(DockerClient client, string imageName)
        {
            try
            {
                var all = await client.Images.ListImagesAsync(new ImagesListParameters() { All = true });
                var existing = all?.FirstOrDefault(o => o.RepoTags?.FirstOrDefault(n => n.Contains(imageName)) != null);
                return existing;
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to get image {imageName} by name ";
                throw new Exception(msg, ex);
            }
        }

        public static async Task<bool> ContainerExistsAsync(DockerClient client, string containerName)
        {
            try
            {
                var ct = await GetContainerByNameAsync(client, containerName);
                return ct != null;
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to check if container {containerName} exists";
                throw new Exception(msg, ex);
            }
        }

        public static async Task<bool> ImageExistsAsync(DockerClient client, string imageName)
        {
            try
            {
                var im = await GetImageByNameAsync(client, imageName);
                return im != null;
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to check if image {imageName} exists";
                throw new Exception(msg, ex);
            }
        }

        public static async Task RemoveContainerByNameAsync(DockerClient client, string containerName)
        {
            try
            {
                var ct = await GetContainerByNameAsync(client, containerName);
                if (ct == null) return;
                await client.Containers.RemoveContainerAsync(ct.ID, new ContainerRemoveParameters() { Force = true });
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to remove container {containerName}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task BuildImageAsync(DockerClient client, string dockerfiletarpackagePath, string imageName = null)
        {
            try
            {
                using (Stream s = new FileStream(dockerfiletarpackagePath, FileMode.Open, FileAccess.Read))
                {
                    await client.Images.BuildImageFromDockerfileAsync(s, new ImageBuildParameters() { Tags = new string[] { imageName } }, CancellationToken.None);
                }
                Thread.Sleep(10000);
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to build image {imageName}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task PullImageAsync(DockerClient client, string imageName, AuthConfig dockerHubAuthInfo = null)
        {
            try
            {
                var split = imageName.Split('/');
                var repo = split.Count() > 0 ? split[0] : null;
                var fullName = imageName; // split.Count() > 0 ? split[1] : imageName;

                split = fullName.Split(':');
                var name = split.Count() > 0 ? split[0] : fullName;
                var tag = split.Count() > 0 ? split[1] : "latest";
                await client.Images.CreateImageAsync(new ImagesCreateParameters { FromImage = name, Tag = tag, Repo = $"{repo}/{name}"},null, new Progress<JSONMessage>(), CancellationToken.None);
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to pull image {imageName}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task LoadImageFromFileAsync(DockerClient client, string imgFilePath)
        {
            try
            {
                if (!File.Exists(Path.GetFullPath(imgFilePath))) throw new ArgumentException("The docker image file does not exist");

                using (Stream fs = new FileStream(Path.GetFullPath(imgFilePath), FileMode.Open, FileAccess.Read))
                {
                    await client.Images.LoadImageAsync(new ImageLoadParameters(), fs, new Progress<JSONMessage>(), CancellationToken.None);
                }

                //await client.Images.CreateImageAsync(new ImagesCreateParameters { FromSrc = imgFilePath }, null, new Progress<JSONMessage>(), CancellationToken.None);
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to load image from path {imgFilePath}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task<CreateContainerResponse> CreateContainerAsync(DockerClient client, string imageName, string containerName, int publicPort, int privatePort, IList<string> envArgs = null)
        {
            try
            {
                var container = await client.Containers.CreateContainerAsync(new CreateContainerParameters
                {
                    Name = containerName,
                    Env = envArgs,
                    Image = imageName,
                    ExposedPorts = new Dictionary<string, EmptyStruct>() { [privatePort.ToString()] = new EmptyStruct() },
                    HostConfig = new HostConfig { PortBindings = new Dictionary<string, IList<PortBinding>> { { privatePort.ToString(), new List<PortBinding> { new PortBinding { HostPort = publicPort.ToString() } } } } }
                });
                return container;
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to create container image:{imageName}, container:{containerName}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task StartContainerAsync(DockerClient client, string containerName, string imageName = null)
        {
            try
            {
                var container = await GetContainerByNameAsync(client, containerName);
                var startParams = new ContainerStartParameters() { };
                if (!string.IsNullOrEmpty(imageName))
                {
                    startParams.DetachKeys += $"d={imageName}";
                }


                if (!await client.Containers.StartContainerAsync(container.ID, startParams, CancellationToken.None))
                {
                    throw new Exception($"Could not start container: {container.ID}");
                };

                var count = 10;
                Thread.Sleep(5000);
                var containerStat = await client.Containers.InspectContainerAsync(container.ID, CancellationToken.None);
                while (!containerStat.State.Running && count-- > 0)
                {
                    Thread.Sleep(1000);
                    containerStat = await client.Containers.InspectContainerAsync(container.ID, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to start container image:{(imageName??"NULL")}, container:{containerName}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task<bool> IsContainerStartedAsync(DockerClient client, string containerName)
        {
            try
            {
                var container = await GetContainerByNameAsync(client, containerName);
                return await IsContainerStartedAsync(client, container);
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to check if container started, container:{containerName}";
                throw new Exception(msg, ex);
            }
        }

        public static async Task<bool> IsContainerStartedAsync(DockerClient client, ContainerListResponse container)
        {
            try
            {
                var containerStat = await client.Containers.InspectContainerAsync(container.ID, CancellationToken.None);
                return containerStat.State.Running;
            }
            catch (Exception ex)
            {
                var msg = $"Docker Helper - Failed to check if container started";
                throw new Exception(msg, ex);
            }
        }

        #endregion static methods
    }  
}
