using DatawarehouseCrawler.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.IntegrationTest.Helper
{
    public static class MockHelper
    {
        public static ImportModel GetDefaultSqlImportDoubleIdTable(Action<Model.ImportModel> options = null)
        {
            var x = new Model.ImportModel()
            {
                Name = "new_salesorderdetail",
                Connection = "src",
                SourceName = "[AdventureWorks2017].[Sales].[SalesOrderDetail]",
                IdFieldName = "[SalesOrderID],[SalesOrderDetailID]",
                Type = "fact",
                //TODO add with join fields
                //InsertQueryDateFieldName = "OrderDate",
                InsertQueryDateFieldName = "ModifiedDate",
                //UpdateQueryDateFieldName = "ModifiedDate"
            };
            if (options != null) options(x);
            return x;
        }

        public static ImportModel GetDefaultSqlImportDoubleIdJoinTable(Action<Model.ImportModel> options = null)
        {
            var x = new Model.ImportModel()
            {
                Name = "new_salesorderdetail",
                Connection = "src",
                SourceName = "[AdventureWorks2017].[Sales].[SalesOrderDetail]",
                IdFieldName = "[SalesOrderID],[SalesOrderDetailID]",
                Type = "fact",
                InsertQueryDateFieldName = "OrderHeader_ModifiedDate",
                Join = new JoinModel[] { new JoinModel() { 
                    Name = "OrderHeader",
                    SourceName = "[AdventureWorks2017].[Sales].[SalesOrderHeader]",
                    IdFieldName = "SalesOrderID", 
                    ParentJoinFieldName = "SalesOrderID", 
                    SelectFields="ModifiedDate",
                    ParentExtraInsertQueryDateFieldName="ModifiedDate" }  
                }
            };
            if (options != null) options(x);
            return x;
        }

        public static ImportModel GetDefaultSqlImportSingleIdTable(Action<Model.ImportModel> options = null)
        {
            var x = new Model.ImportModel()
            {
                Name = "new_salesorderheader",
                Connection = "src",
                SourceName = "[AdventureWorks2017].[Sales].[SalesOrderHeader]",
                IdFieldName = "[SalesOrderID]",
                Type = "fact",
                InsertQueryDateFieldName = "OrderDate",
                UpdateQueryDateFieldName = "ModifiedDate"
            };
            if (options != null) options(x);
            return x;
        }


        public static ImportModel GetDefaultODataImportSingleIdTable(Action<Model.ImportModel> options = null)
        {
            var x = new Model.ImportModel()
            {
                Name = "new_salesorderdetail",
                Connection = "src",
                SourceName = "Order",
                IdFieldName = "OrderID",
                Type = "fact",
                InsertQueryDateFieldName = "OrderDate",
                UpdateQueryDateFieldName = "RequiredDate",
                SourceTypeEnum = SourceTypeEnum.odata,
                QuerySubUrl = "Orders"
            };
            if (options != null) options(x);
            return x;
        }

        public static ImportModel GetDefaultExcelImportSingleIdTable(Action<Model.ImportModel> options = null)
        {
            var x = new Model.ImportModel()
            {
                Name = "new_salesorderheader",
                Connection = "src",
                SourceName = "SalesOrderHeader",
                IdFieldName = "SalesOrderID",
                Type = "fact",
                InsertQueryDateFieldName = "OrderDate",
                UpdateQueryDateFieldName = "ModifiedDate",
                SourceTypeEnum = SourceTypeEnum.dataset
            };
            if (options != null) options(x);
            return x;
        }
    }
}
