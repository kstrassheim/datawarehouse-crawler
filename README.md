# Datawarehouse-Crawler (2018)
This application is a content and schema crawler tool to receive, update and import various kinds of data into a On-Premise or Cloud based SQLServer or Azure-Synapse-Analytics (Azure Datawarehouse SQLServer). As source it supports SQLServer Tables, ODATA Endpoints, CSV Files or Excel Files. For multiple sources it can run in parallel mode where it would make a thread for each connection. The specialty of this crawler is that it creates the target tables by himself using the additional info from source.json. In case of Azure-Synapse-Analytics it would estimate the distribution type and keys. The syncing works completely without SQL Transactions by using a consistency correction algorithm for very frequent fact tables. There are 5 Syncing Algorithms (see Manual/Insert) which can be selected as well as one Update Algorithm. 

## Purpose
The purpose and invariant for every run is very simple. "Just take an empty datawarehouse without any tables. Run the programm. After the run all tables and data will be up to date __automatically__.

## Abstract-Keywords
BI, Datawarehouse, Crawler, Data-Import, Parallel-Computing, Integration-Testing, Datawarehousing, Agile-Data-Science, Agile-Business-Intelligence

## Technical Keywords
.NET Core 2.2, C#, Azure-Synapse-Analytics, Azure-Datawarehouse, Docker, SQL, SQLServer, ODATA, Excel, CSV, Azure-Functions, .NET-Parallel-Computing

## Manual (Short)

### Insert
in source.json set the following values as "dataSyncType"
1. appendbyidexclude - will create excluding id rules in the where clause
2. appendbyid - requires "idFieldName" to be set. Will expect that the id is incremental and just query > largest id - very fast - recommended if possible but no updates
3. appendbyidfirst - when entering multiple id fields for seperation, the first id will be used for the algorithm, the other fields for clear seperation. (Useful for special sql tables with no clear id or InsertDates)
4. appendbydate - requires "insertQueryDateFieldName" to be set. will use one or multiple date fields (Created-Date would be a good choice) insertQueryDateFieldName - When entering multiple fields (If year, month are seperated or integers) make sure your enter them in the right order (Year, Month, Day, Hour). The algorithm will stack them together. (Optional) you can set "insertQueryDateFormat" for some value like "yyyy-MM-dd HH:mm:ss.0000000" if you have to parse the date.
5. appendbydatestrict - Used where the timestamp is from different size than SQL. Should be used for sources like Excel.

### Update

has additionaly one update mode if required by setting "updateMode":"updateByModifiedDate" and "updateQueryDateFieldName": "Date field". If the correct Modified indicator is applied the Datawarehouse will also update its imported values.
	
### Target SQL Server 
If the source and target are both SQLServers and you don't explizitly  set the AvoidCreatingPrimaryKeysInSchema=true to the target data adapter the DwhCrawler will take over the primary keys into the datawarehouse on the first import.

### Azure-Synapse-Analytics settings
By setting IsAzureDwh=True to the SqlDataAdapter you will select Azure-Synapse-Analytics as the target table. 
There are following simple settings for it Columnstore:bool, DistributionType:{Hash = 0, RoundRobin = 1, Replicated = 2}, ExplizitIgnoreIdentity.
For the rest please check the code in file DatawarehouseCrawler\DataAdapters\SqlDataAdapter. 

### Runtime configuration
You can set the runtime settings whether via the following way ConsoleArguments, AppSettings, EnvironmentVariables. The prio for overriding will have the same order while the first has highest.
A list of all runtime settings you can see at DatawarehouseCrawler\Runtime\ImporterRuntimeSettings.

## Installation
1. Download and install .NET Core 2.2 SDK https://dotnet.microsoft.com/download/dotnet/2.2 
2. Download and install Docker Desktop https://hub.docker.com/editions/community/docker-ce-desktop-windows
3. Download and install Visual Studio Community Edition
3. Clone repository
4. Start Docker Desktop and make sure the deamon runs
5. Create Testing Docker container by running DatawarehouseCrawler.IntegrationTest\container\create.cmd . This will create a Docker image SQL Server Linux instance with AdventureWorks2017 example database. !!! If you have SQL Connection issues by creating the container just run it twice.
6. Make sure Port 9443 is free or change it in the files of IntegrationTest project. 
7. Open DatawarehouseCrawler.sln in the root folder
8. Build the whole Solution and make sure there are no errors
9. Right click on the IntegrationTest project then "Run All Tests" to check if the 83 Tests are running and proving that the program works as expected. 

## Runtime Configuration
1. Go to your Azure Portal and create a Key-Vault a Storage and an Application Insights project afterwards parse the keys and the TenantId into the following 2 setup files 
	1. DatawarehouseCrawler.Console\App.config
	2. DatawarehouseCrawler.AzureFunction\local.settings.json
2. Add your source connection strings to the Azure Key-Vault
3. Edit source table links DatawarehouseCrawler.Console\source.json file 
	1. Add all your source connection tables by its type
	2. The connection attributes is directly mapped to the key vault.
	3. To every source you should add its source name its target name, information about the size or type of the datawarehouse table (fact, or dim) 

For a suggestion on how-to publish your project using CI/CD please check out .\pipeline-deploy-azure-function.yml file
