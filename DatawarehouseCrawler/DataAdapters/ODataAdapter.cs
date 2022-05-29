using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Xml;
using DatawarehouseCrawler.Model.Query;
using DatawarehouseCrawler.QueryAdapters;

namespace DatawarehouseCrawler.DataAdapters
{
    public class ODataRequestProvider
    {
        protected string AuthenticationHeader { get; set; }

        public ODataRequestProvider() { }

        public ODataRequestProvider(string username, string password)
        {
            this.Init(username, password);
        }

        public void Init(string username, string password)
        {
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                this.AuthenticationHeader = $"Basic {System.Convert.ToBase64String(System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(username + ":" + password))}";
            }
        }

        public virtual Stream GetResponseFromServer(string url)
        {
            var client = this.GetClient(url);
            var response = client.GetResponse();
            var ms = new MemoryStream();
            response.GetResponseStream().CopyTo(ms);
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        public string GetStringFromServer(string url)
        {
            var resp = this.GetResponseFromServer(url);
            string value = string.Empty;
            using (var sr = new StreamReader(resp)) { value = sr.ReadToEnd(); }
            return value;
        }

        public XmlDocument GetXmlFromServer(string url)
        {
            var resp = this.GetResponseFromServer(url);
            var doc = new XmlDocument();
            doc.Load(resp);
            return doc;
        }

        protected HttpWebRequest GetClient(string url)
        {
            var c = (HttpWebRequest)WebRequest.Create(url);
            if (!string.IsNullOrEmpty(this.AuthenticationHeader))
            {
                c.Headers.Add("Authorization", this.AuthenticationHeader);
            }
            return c;
        }
    }

    public class ODataAdapter : ISourceDataAdapter
    {
        public Table Table { get; protected set; }

        private ODataQueryAdapter queryAdapter = new ODataQueryAdapter();

        public string DefaultQueryParamsSuffix
        {
            get { return this.queryAdapter.DefaultQueryParamsSuffix; }
            set { this.queryAdapter.DefaultQueryParamsSuffix = value; }
        }

        protected ODataQueryAdapter QueryAdapter { get { return this.queryAdapter; } }

        public ODataAdapter(Table table, string connectionString, string customSubUrlName = null)
        {
            this.Table = table;
            var username = this.ExtractConnectionStringProperty(connectionString, "User ID", false);
            var password = this.ExtractConnectionStringProperty(connectionString, "Password", false);
            this.ODataQueryProvider = new ODataRequestProvider(username, password);
            string pagesize = this.ExtractConnectionStringProperty(connectionString, "Pagesize", false);
            if (!string.IsNullOrEmpty(pagesize)) { this.CustomPageSize = int.Parse(pagesize); }
            this.ServiceUrl = this.ExtractConnectionStringProperty(connectionString, "URL")?.TrimEnd('/');
            this.Url = $"{this.ServiceUrl}/{(customSubUrlName ?? string.Empty)}";
        }

        public void Dispose()
        {
        }

        #region properties

        public int? CustomPageSize { get; private set; }

        protected readonly static Dictionary<string, SqlDbType> EdmSqlTypeMap = new Dictionary<string, SqlDbType>() {
            {"Edm.Int64", SqlDbType.BigInt },
            {"Edm.Binary", SqlDbType.Binary},
            {"Edm.Boolean", SqlDbType.Bit },
            {"Edm.String", SqlDbType.NVarChar },
            {"Edm.Date", SqlDbType.Date },
            {"Edm.DateTime", SqlDbType.DateTime },
            {"Edm.Decimal", SqlDbType.Decimal },
            {"Edm.Double", SqlDbType.Float },
            {"Edm.Int32", SqlDbType.Int },
            {"Edm.Single", SqlDbType.Real },
            {"Edm.Int16", SqlDbType.SmallInt },
            {"Edm.TimeOfDay", SqlDbType.Time },
            {"Edm.DateTimeOffset", SqlDbType.Timestamp },
            {"Edm.Byte", SqlDbType.TinyInt },
            {"Edm.SByte3", SqlDbType.TinyInt }
        };

        protected readonly static Dictionary<string, Type> EdmTypeMap = new Dictionary<string, Type>() {
            {"Edm.Int64", typeof(long) },
            {"Edm.Boolean", typeof(bool) },
            {"Edm.String", typeof(string) },
            {"Edm.Date", typeof(DateTime) },
            {"Edm.DateTime", typeof(DateTime) },
            {"Edm.Decimal", typeof(decimal) },
            {"Edm.Double", typeof(double) },
            {"Edm.Int32", typeof(int) },
            {"Edm.Binary", typeof(byte[]) },
            {"Edm.Single", typeof(Single) },
            {"Edm.Int16", typeof(short) },
            {"Edm.TimeOfDay", typeof(DateTime) },
            {"Edm.DateTimeOffset", typeof(DateTimeOffset) },
            {"Edm.Byte", typeof(byte) },
            {"Edm.SByte3", typeof(byte) }
        };

        protected string ServiceUrl { get; set; }

        protected string Url { get; set; }

        public ODataRequestProvider ODataQueryProvider { get; set; }

        private DataSet QueryDataSet { get; set; }

        private Dictionary<string, DataColumn> QueryColumns { get; set; }

        #endregion properties

        #region methods

        public string[] GetPrimaryKeys()
        {
            var ds = this.GetSchema();
            return ds.PrimaryKey?.Select(p => p.ColumnName)?.ToArray();
        }

        protected DataTable GenerateDatatableSchema(string tableName, IEnumerable<string> keys, Dictionary<string, Dictionary<string, string>> fields)
        {
            var dt = new DataTable(tableName);
            foreach (var k in fields.Keys)
            {
                var v = fields[k];
                var c = new DataColumn(k);
                if (v.ContainsKey("Type"))
                {
                    var t = v["Type"];
                    var split = t.Split(".");
                    if (t.Length > 1 && split[0].ToLower() == "edm")
                    {
                        c.DataType = EdmTypeMap[t];
                    }
                    else
                    {
                        throw new ArgumentException("Other types than edm are not supported not for odata");
                    }
                }

                if (v.ContainsKey("MaxLength"))
                {
                    int l = int.Parse(v["MaxLength"]);
                    c.MaxLength = l;
                }
                else if (c.DataType == typeof(string))
                {
                    c.MaxLength = 4000;
                }

                if (v.ContainsKey("Nullable"))
                {
                    bool n = bool.Parse(v["Nullable"]);
                    c.AllowDBNull = n;
                }

                dt.Columns.Add(c);

                if (keys.Contains(k))
                {
                    dt.PrimaryKey = dt.PrimaryKey != null ? dt.PrimaryKey.Append(c).ToArray() : new DataColumn[] { c };
                }
            }

            return dt;
        }

        private void InsertCastedColumnValue(ref DataRow dr, string colName, DataColumn c, string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                if (!c.AllowDBNull) { throw new ArgumentException($"Value cannot be null in column {colName}"); }
                else { return; }
            }

            if (c.DataType == typeof(int)) { dr[colName] = int.Parse(value); }
            else if (c.DataType == typeof(string)) { dr[colName] = value; }
            else if (c.DataType == typeof(bool)) { dr[colName] = bool.Parse(value); }
            else if (c.DataType == typeof(DateTime)) { dr[colName] = DateTime.Parse(value); }
            else if (c.DataType == typeof(decimal)) { dr[colName] = decimal.Parse(value); }
            else if (c.DataType == typeof(double)) { dr[colName] = double.Parse(value); }
            else if (c.DataType == typeof(long)) { dr[colName] = long.Parse(value); }
            else if (c.DataType == typeof(byte[])) { dr[colName] = System.Text.Encoding.Default.GetBytes(value); }
            else if (c.DataType == typeof(Single)) { dr[colName] = Single.Parse(value); }
            else if (c.DataType == typeof(short)) { dr[colName] = short.Parse(value); }
            else if (c.DataType == typeof(DateTimeOffset)) { dr[colName] = DateTimeOffset.Parse(value); }
            else if (c.DataType == typeof(byte)) { dr[colName] = byte.Parse(value); }
        }

        private string ExtractFirstColumnValue(string query, string searchColumnName)
        {
            var doc = this.ODataQueryProvider.GetXmlFromServer(query);
            return this.ExtractColumnValue(doc, searchColumnName);
        }

        private string ExtractColumnValue(XmlDocument doc, string searchColumnName)
        {
            var entries = doc.GetElementsByTagName("entry");
            //var partialrows = entries.Count;
            //var fields = new Dictionary<string, Dictionary<string, string>>();
            //var keys = new List<string>();

            foreach (XmlNode e in entries)
            {
                foreach (XmlNode ec in e.ChildNodes)
                {
                    if (ec.Name == "content")
                    {
                        foreach (XmlNode ecc in ec.ChildNodes)
                        {
                            if (ecc.Name == "m:properties")
                            {
                                foreach (XmlNode p in ecc.ChildNodes)
                                {
                                    string colName = p.Name.TrimStart('d').TrimStart(':');
                                    if (colName == searchColumnName)
                                    {
                                        return p.InnerText;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return null;
        }

        private IEnumerable<IEnumerable<string>> ExtractColumnValues(XmlDocument doc, IEnumerable<string> searchColumnNames)
        {
            var ret = new List<IEnumerable<string>>();
            if (searchColumnNames == null) { return ret; }
            var entries = doc.GetElementsByTagName("entry");
            //var partialrows = entries.Count;
            //var fields = new Dictionary<string, Dictionary<string, string>>();
            //var keys = new List<string>();
            
            foreach (XmlNode e in entries)
            {
                var inner = new List<string>();
                foreach (XmlNode ec in e.ChildNodes)
                {
                    if (ec.Name == "content")
                    {
                        foreach (XmlNode ecc in ec.ChildNodes)
                        {
                            if (ecc.Name == "m:properties")
                            {
                                foreach (XmlNode p in ecc.ChildNodes)
                                {
                                    string colName = p.Name.TrimStart('d').TrimStart(':');
                                    if (searchColumnNames.Contains(colName))
                                    {
                                         inner.Add(p.InnerText);
                                    }
                                }
                            }
                        }
                    }
                }

                ret.Add(inner);
            }

            return ret;
        }

        public string ExtractConnectionStringProperty(string connectionString, string property, bool mustexist = true)
        {
            var split = connectionString.Split(";");
            foreach (var s in split)
            {
                var isplit = s.Split('=');
                if (isplit[0]?.Trim().ToLower() == property.Trim().ToLower())
                {
                    return isplit[1];
                };
            }
            if (mustexist) { throw new ArgumentException("GetConnectionStringProperty - Could not find property in connection string"); }
            else return null;
        }

        protected string AppendGetParameter(string url, string fullparam)
        {
            if (string.IsNullOrEmpty(fullparam)) return url;
            var split = fullparam.Split('=');
            return this.AppendGetParameter(url, split[0], split[1]);
        }

        protected string AppendGetParameter(string url, string name, string value, bool overwrite = false)
        {
            string n = name.Trim(), v = value.Trim();
            if (url.Contains('?'))
            {
                if (url.Contains(n))
                {
                    if (overwrite)
                    {
                        throw new NotImplementedException("overwriting url param is not implemented yet");
                    }
                }
                else
                {
                    return $"{url}&{n}={v}";
                    //return $"{url}&{HttpUtility.UrlEncode(n)}={HttpUtility.UrlEncode(v)}";
                }
            }
            else
            {
                return $"{url}?{n}={v}";
                //return $"{url}?{HttpUtility.UrlEncode(n)}={HttpUtility.UrlEncode(v)}";
            }

            return url;
        }

        #endregion methods

        public int GetCount(Condition filter = null)
        {
            var query = $"{this.Url?.TrimEnd('/')}/$count";
            var filterStr = this.QueryAdapter.ConvertCondition(filter);
            query=this.AppendGetParameter(query, !string.IsNullOrEmpty(filterStr) ? $"$filter={filterStr}" : null);

            if (!string.IsNullOrEmpty(this.DefaultQueryParamsSuffix))
            if (query.Contains('?'))
            {
                query = $"{query}&{this.DefaultQueryParamsSuffix}";
            }
            else
            {
                query = $"{query}?{this.DefaultQueryParamsSuffix}";
            }

            return int.Parse(this.ODataQueryProvider.GetStringFromServer(query.ToString()));
        }

        public Task<object> GetCountAsync(Condition filter = null)
        {
            var t = new Task<object>(() => this.GetCount(filter));
            t.Start();
            return t;
        }

        public IEnumerable<IEnumerable<object>> GetFieldValues(SelectQuery selectQuery)
        {
            var ret = new List<IEnumerable<object>>();
            var ds = this.RunQuery(selectQuery);
            foreach(DataRow dr in ds?.Tables[0].Rows)
            {
                var inner = new List<object>();
                foreach(var col in selectQuery.Columns)
                {
                    inner.Add(dr[col.InternalName]);
                }

                ret.Add(inner);
            }

            return ret;
        }

        public IEnumerable<IEnumerable<object>> GetFieldValues(IEnumerable<Column> fields, Condition filter = null)
        {
            var filterStr = this.QueryAdapter.ConvertCondition(filter);
            var query = !string.IsNullOrEmpty(filterStr) ? $"{this.Url?.TrimEnd('/')}?$filter={filterStr}" : this.Url?.TrimEnd('/');
            var doc = this.ODataQueryProvider.GetXmlFromServer(query);
            return this.ExtractColumnValues(doc, fields.Select(c=>c.Name));
        }
        public Task<IEnumerable<IEnumerable<object>>> GetFieldValuesAsync(SelectQuery selectQuery)
        {
            var t = new Task<IEnumerable<IEnumerable<object>>>(() => this.GetFieldValues(selectQuery));
            t.Start();
            return t;
        }
        public object GetMaxFieldValue(Column sourceField, Condition c = null)
        {
            var ret = this.ExtractFirstColumnValue(this.Url?.TrimEnd('/') + $"?$orderby={sourceField.Name} desc&$top=1&$select={sourceField.Name}", sourceField.Name);
            return ret;
        }

        public DataTable GetSchema(IEnumerable<Column> columns = null)
        {
            var doc = this.ODataQueryProvider.GetXmlFromServer($"{this.ServiceUrl?.TrimEnd('/')}/$metadata");
            var schemaTags = doc.GetElementsByTagName("Schema");

            var fields = new Dictionary<string, Dictionary<string, string>>();
            var keys = new List<string>();

            foreach (XmlNode sc in schemaTags)
            {
                foreach (XmlNode entityType in sc.ChildNodes)
                {
                    if (entityType.Name == "EntityType")
                    {
                        foreach (XmlAttribute a1 in entityType.Attributes)
                        {
                            if (a1.Name == "Name" && a1.Value == this.Table.Name)
                            {
                                foreach (XmlNode prop in entityType.ChildNodes)
                                {
                                    var name = string.Empty;
                                    var field = new Dictionary<string, string>();
                                    if (prop.Name == "Property")
                                    {
                                        foreach (XmlAttribute a in prop.Attributes)
                                        {
                                            if (a.Name == "Name")
                                            {
                                                name = a.Value;
                                            }
                                            else
                                            {
                                                field.Add(a.Name, a.Value);
                                            }
                                        }
                                    }
                                    else if (prop.Name == "Key")
                                    {
                                        foreach (XmlNode key in prop.ChildNodes)
                                        {
                                            if (key.Name == "PropertyRef")
                                            {
                                                foreach (XmlAttribute ak in key.Attributes)
                                                {
                                                    if (ak.Name == "Name")
                                                    {
                                                        keys.Add(ak.Value);
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if (!string.IsNullOrEmpty(name))
                                    {
                                        fields.Add(name, field);
                                    }
                                }

                                break;
                            }
                        }
                    }
                }
            }

            return this.GenerateDatatableSchema(this.Table.Name, keys, fields);
        }

        public IEnumerable<ColumnTypeInfo> GetExtendedColumnTypeInfo(string tableName)
        {
            return new List<ColumnTypeInfo>();
        }

        public DataSet RunQuery(SelectQuery query)
        {
            string q = $"{this.Url?.TrimEnd('/')}{this.QueryAdapter.ConvertSelectQuery(query)}";
            //throw new NotImplementedException();
            if (this.QueryDataSet == null)
            {
                this.QueryDataSet = new DataSet();
                this.QueryDataSet.Tables.Add(this.GetSchema());
            }

            var dt = this.QueryDataSet.Tables[0].Clone();
            // save columns in dictionary
            Dictionary<string, DataColumn> cols = new Dictionary<string, DataColumn>();
            foreach (DataColumn c in dt.Columns) { cols.Add(c.ColumnName, c); }

            dt.Rows.Clear();

            var doc = this.ODataQueryProvider.GetXmlFromServer(q);
            var entries = doc.GetElementsByTagName("entry");
            //var partialrows = entries.Count;
            //var fields = new Dictionary<string, Dictionary<string, string>>();
            //var keys = new List<string>();

            foreach (XmlNode e in entries)
            {
                foreach (XmlNode ec in e.ChildNodes)
                {
                    if (ec.Name == "content")
                    {
                        foreach (XmlNode ecc in ec.ChildNodes)
                        {
                            if (ecc.Name == "m:properties")
                            {
                                var dr = dt.NewRow();

                                foreach (XmlNode p in ecc.ChildNodes)
                                {
                                    string colName = p.Name.TrimStart('d').TrimStart(':');
                                    this.InsertCastedColumnValue(ref dr, colName, cols[colName], p.InnerText);
                                }

                                dt.Rows.Add(dr);
                            }
                        }
                    }
                }
            }
            var ds = new DataSet();
            ds.Tables.Add(dt);
            return ds;
        }

        public Task<DataSet> RunQueryAsync(SelectQuery query)
        {
            Task<DataSet> t = new Task<DataSet>(() => { return this.RunQuery(query); });
            t.Start();
            return t;
        }
    }
}
