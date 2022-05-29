using DatawarehouseCrawler.IntegrationTest.Helper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.IntegrationTest
{
    [TestClass]
    public static class AssemblyClass
    {
        #if (DEBUG)
            private static readonly bool keepOpen = true;
        #else
            private static readonly bool keepOpen = false;
        #endif

        private static readonly DockerHelper docker = new DockerHelper("kstrassheim/mssql-server-linux-adventureworks:latest", "mssql-server-linux-adventureworks", 9433, 1433) {
            ImageFilePath = "../../../container/mssql-server-linux-adventureworks"
        };

        [AssemblyInitialize()]
        public static async Task AsmInitialize(TestContext testContext)
        {
            try
            {
                if (keepOpen)
                {
                    try
                    {
                        if (await docker.IsStarted()) { return; }
                    }
                    catch { }
                }

                await docker.Start();
                Thread.Sleep(10000);
            }
            catch
            {
                throw;
            }
        }

        [AssemblyCleanup]
        public static async Task AsmCleanup()
        {
            if (!keepOpen) { await docker.Stop(); }
        }
    }
}
