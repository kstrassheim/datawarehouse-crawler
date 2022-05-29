using DatawarehouseCrawler.Providers.FileStreamProviders;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Xml;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.Providers.DataSetProviders
{
    public class ExcelXmlDataSetProvider : IDisposable, IDataSetProvider
    {
        #region nested types

        enum TypeEnum
        {
            None = 0,
            Boolean = 1,
            DateTime = 2,
            Guid = 3,
            Number = 4,
            String = 5
        }

        enum NumericTypeEnum
        {
            None = 0,
            Int = 1,
            Long = 2,
            Decimal = 3,
            Double = 4
        }

        class ColNode
        {
            public long Index { get; set; }
            public string Name { get; set; }

            public override string ToString()
            {
                return $"{this.Index} {this.Name}";
            }
        }

        class ValueNode
        {
            public bool IsNull { get; set; }

            public int RowIndex { get; set; }

            public int ColIndex { get; set; }

            public int OriginalOffset { get; set; }

            public int RealColIndex { get; set; }

            public int Offset { get; set; }

            public string Value { get; set; }

            public TypeEnum Type { get; set; }

            public NumericTypeEnum NumericType { get; set; }

            public int Length { get; set; }

            public override string ToString()
            {
                return $"{this.Value} {this.Type.ToString("f")} {this.NumericType.ToString("f")} {this.ColIndex}+{this.Offset}={this.RealColIndex} {this.OriginalOffset}";
            }
        }

        #endregion nested types

        private ParallelOptions parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount };

        protected FileStreamProviders.IStreamProvider StreamProvider { get; set; }

        public string TableName { get; set; }

        private DataSet dataSet = null;

        private bool initialized = false;
        public ushort MaxFieldLength { get; set; } = 4000;

        public byte LengthTolerance { get; set; } = 30;

        public bool ForceAllFieldsToAllowNull { get; set; } = false;

        public DataSet DataSet
        {
            get
            {
                if (!this.initialized) { this.Init(); }
                return this.dataSet;
            }
        }

        public TimeSpan InitProcessTime { get; set; }

        public virtual void Init()
        {

            var numTypeDict = new Dictionary<NumericTypeEnum, Type>() {
                {NumericTypeEnum.Int, typeof(int) },
                {NumericTypeEnum.Long, typeof(int) },
                {NumericTypeEnum.Decimal, typeof(decimal) },
                {NumericTypeEnum.Double, typeof(double) },
                {NumericTypeEnum.None, typeof(double) },
            };

            var typeDict = new Dictionary<TypeEnum, Type>() {
                {TypeEnum.None, typeof(string) },
                {TypeEnum.String, typeof(string) },
                {TypeEnum.DateTime, typeof(DateTime) },
                {TypeEnum.Boolean, typeof(bool) },
                {TypeEnum.Guid, typeof(Guid) }
            };

            #region read xml
            List<ColNode> cols = new List<ColNode>();
            List<ValueNode> vals = new List<ValueNode>();

            var initTime = System.Diagnostics.Stopwatch.StartNew();

            string iws = null;
            using (var stream = this.StreamProvider.GetStream())
            {
                stream.Position = 0;
                var doc = new XmlDocument();
                doc.Load(stream);

                //doc.Load(new XmlTextReader(stream));
                var nsmgr = new XmlNamespaceManager(doc.NameTable);

                nsmgr.AddNamespace("o", "urn:schemas-microsoft-com:office:office");
                nsmgr.AddNamespace("x", "urn:schemas-microsoft-com:office:excel");
                nsmgr.AddNamespace("ss", "urn:schemas-microsoft-com:office:spreadsheet");

                var node = doc.DocumentElement.SelectSingleNode($"//ss:Worksheet[@ss:Name='{this.TableName}']", nsmgr);

                //var colRows = 
                var colNodes = node.SelectNodes("ss:Table/ss:Row[1]", nsmgr).AsParallel().Cast<XmlNode>();
                var valNodes = node.SelectNodes("ss:Table/ss:Row", nsmgr).AsParallel().Cast<XmlNode>().Skip(1);

                Action<ParallelQuery<XmlNode>, Action<XmlNode, long, long, int>> processRows = (rows, a) =>
                {
                    Parallel.ForEach(rows.Cast<XmlNode>(), this.parallelOptions, (r, sr, i) =>
                    {
                        var cells = r.SelectNodes("ss:Cell", nsmgr)?.AsParallel();
                        var j = 0;
                        foreach (XmlNode c in cells)
                        {
                            int offset = 0;
                            if (c.Attributes["ss:Index"] != null)
                            {
                                offset = int.Parse(c.Attributes["ss:Index"]?.Value);
                            }
                            var d = c.SelectSingleNode("ss:Data", nsmgr);
                            a(d, i, j, offset);
                            j++;
                        }
                    });
                };
                var iw = System.Diagnostics.Stopwatch.StartNew();
                processRows(colNodes, (n, i, j, offset) => { var c = new ColNode() { Index = j, Name = n?.InnerText }; lock (cols) { cols.Add(c); } });
                processRows(valNodes, (n, i, j, offset) =>
                {
                    var v = new ValueNode()
                    {
                        RowIndex = (int)i,
                        ColIndex = (int)j,
                        OriginalOffset = offset,
                        //Offset = offset > (int)j ? offset - (int)j - 1 : 0,
                        Value = n?.InnerText,
                        Length = n?.InnerText?.Length ?? -1,
                        IsNull = n?.InnerText == null,
                        Type = (TypeEnum)Enum.Parse(typeof(TypeEnum), n?.Attributes["ss:Type"]?.Value ?? TypeEnum.None.ToString("D"))
                    };
                    lock (vals) { vals.Add(v); }

                });
                iw.Stop();
                iws = iw.Elapsed.ToString();
            }

            #endregion readxml
            
            var rwlen = vals.Max(o => o.RowIndex) + 1;
            // increase offset and set real col offset
            Parallel.For(0, rwlen, this.parallelOptions, (i) =>
            {
                var r = vals.Where(o => o.RowIndex == i).OrderBy(o => o.ColIndex);
                int offset = 0;
                foreach(var c in r)
                {
                    if (c.OriginalOffset > c.ColIndex)
                    {
                        c.RealColIndex = c.OriginalOffset - 1;
                        c.Offset = c.RealColIndex - c.ColIndex;
                        offset += c.Offset - offset;
                    }
                    else if (c.OriginalOffset < 1)
                    {
                        c.Offset = offset;
                        c.RealColIndex = c.ColIndex + offset;
                    }
                }
            });

            // get numeric types
            Parallel.ForEach(vals.Where(o => o.Type == TypeEnum.Number), this.parallelOptions, n =>
            {
                if (n.IsNull) { return; }

                if (int.TryParse(n.Value, out int num))
                {
                    n.NumericType = NumericTypeEnum.Int;
                    return;
                }

                if (long.TryParse(n.Value, out long nl))
                {
                    n.NumericType = NumericTypeEnum.Int;
                    return;
                }

                if (decimal.TryParse(n.Value, out decimal dec))
                {
                    n.NumericType = NumericTypeEnum.Decimal;
                    return;
                }

                n.NumericType = NumericTypeEnum.Double;
            });

            Parallel.ForEach(vals.Where(o => o.Type == TypeEnum.String), this.parallelOptions, n =>
            {
                if (n.IsNull) { return; }
                Guid num = Guid.Empty;
                if (Guid.TryParse(n.Value, out num))
                {
                    n.Type = TypeEnum.Guid;
                    return;
                }
            });

            // init columns
            var orderedVals = vals.GroupBy(o => o.RealColIndex);
            
            DataColumn[] dcols = new DataColumn[cols.Count()];

            Parallel.ForEach(orderedVals, this.parallelOptions, (cv) =>
            {
                var c = cols[cv.Key];
                var t = cv.Max(o => o.Type);
                var nt = t == TypeEnum.Number ? cv.Max(o => o.NumericType) : NumericTypeEnum.None;
                var l = -1;
                if (t == TypeEnum.String || t == TypeEnum.None) { 
                    var ml = cv.Max(o => o.Length); 
                    ml = ml > 0 ? ml + this.LengthTolerance : ml; 
                    l = ml > 0 && ml < this.MaxFieldLength ? ml : this.MaxFieldLength; 
                }
                dcols[cv.Key] = new DataColumn(c.Name, t == TypeEnum.Number ? numTypeDict[nt] : typeDict[t]) { MaxLength = l, AllowDBNull = this.ForceAllFieldsToAllowNull || cv.Count() < rwlen || cv.Any(o => o.IsNull) };
            });

            DataTable dt = new DataTable(this.TableName);
            dt.Columns.AddRange(dcols);
            
            DataRow[] drows = new DataRow[rwlen];
            for(var i = 0; i < rwlen; i++)
            {
                drows[i] = dt.NewRow();
            }

            // Parse Values
            Parallel.For(0, rwlen, this.parallelOptions, (i) =>
            {
                var dr = drows[i];
                Action<object, int> assign = (val, k) => { lock (dt) { dr[k] = val; } };
                var values = vals.Where(o => o.RowIndex == i).OrderBy(o=>o.RealColIndex).ToArray();
                int offset = 0;
                foreach (var v in values)
                {
                    if (v.Offset > 0) offset += v.Offset;
                    var j = v.RealColIndex;
                    var dc = dcols[v.RealColIndex];
                    
                    if (v == null || v.IsNull)
                    {
                        continue;
                    }
                    if (dc.DataType == typeof(string))
                    {
                        assign(v.Value, j);
                        continue;
                    }
                    if (dc.DataType == typeof(int))
                    {
                        assign(int.Parse(v.Value), j);
                        continue;
                    }
                    if (dc.DataType == typeof(long))
                    {
                        assign(long.Parse(v.Value), j);
                        continue;
                    }
                    if (dc.DataType == typeof(decimal))
                    {
                        assign(decimal.Parse(v.Value), j);
                        continue;
                    }
                    if (dc.DataType == typeof(double))
                    {
                        assign(double.Parse(v.Value), j);
                        continue;
                    }
                    if (dc.DataType == typeof(DateTime))
                    {
                        assign(DateTime.Parse(v.Value), j);
                        continue;
                    }
                    if (dc.DataType == typeof(bool))
                    {
                        assign(v.Value == "1", j);
                        continue;
                    }
                    if (dc.DataType == typeof(Guid))
                    {
                        assign(Guid.Parse(v.Value), j);
                        continue;
                    }
                }

            });

            foreach(var r in drows)
            {
                dt.Rows.Add(r);
            }

            var ds = new DataSet();
            ds.Tables.Add(dt);
            this.dataSet = ds;
            this.initialized = true;
            initTime.Stop();

            this.InitProcessTime = initTime.Elapsed;
        }

        public void Dispose()
        {
            this.dataSet?.Dispose();
            this.dataSet = null;
            this.initialized = false;
        }

        public ExcelXmlDataSetProvider(FileStreamProviders.IStreamProvider streamProvider, string worksheetName)
        {
            this.StreamProvider = streamProvider;
            this.TableName = worksheetName;
        }
    }
}
