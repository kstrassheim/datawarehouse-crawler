using System;
using System.IO;
using System.Configuration;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text.RegularExpressions;
using DatawarehouseCrawler.Model;
using LOG = DatawarehouseCrawler.Model.Log;
using System.Threading.Tasks;
using System.Threading;
using DatawarehouseCrawler.IntegrationTest.Helper;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler;
using DatawarehouseCrawler.QueryAdapters;

namespace DatawarehouseCrawler.IntegrationTest
{
    [TestClass]
    public class SqlNoKeysImporterTest : SqlImporterTest
    {
        public override DataAdapters.SqlDataAdapter GetTargetDataAdapter(Model.ImportModel x, SqlConnection con)
        {
            var a = base.GetTargetDataAdapter(x, con);
            a.AvoidCreatingPrimaryKeysInSchema = true;
            return a;
        }

        public override void ApplyAfterSchemaTest(DataImporter importer, SqlConnection src, SqlConnection tgt)
        {
            var targetPkeys = SqlHelper.getPrimaryKeys(importer.Import.Name, tgt);
            Assert.AreEqual(0, targetPkeys.Count(), "Primary key creation should be disabled");
        }

        public override void ApplyAfterDataTest(DataImporter importer)
        {
            bool duplicateKeysExist = importer.HasTargetDuplicateKeys();
            Assert.IsFalse(duplicateKeysExist, "There are duplicate keys on target");
        }

    }
}
