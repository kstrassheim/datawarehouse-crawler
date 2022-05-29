using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.Providers.DataSetProviders
{
    public class CsvDataSetProvider : IDisposable, IDataSetProvider
    {
        #region nested types

        enum TypeEnum
        {
            None = 0,
            Boolean = 1,
            DateTime = 2,
            Guid = 3,
            Int = 4,
            Long = 5,
            Decimal = 6,
            Double = 7,
            String = 8
        }

        #endregion nested types

        //public char RowSeperator { get; set; } = '\n';
        public char ColSeperator { get; set; } = ';';

        public Dictionary<string, bool> CustomBooleanValues { get; set; } = new Dictionary<string, bool> { { "wahr", true }, { "falsch", false }, { "1", true }, { "0", false } };

        public ushort MaxFieldLength { get; set; } = 4000;

        public byte LengthTolerance { get; set; } = 0;

        public bool ForceAllFieldsToAllowNull { get; set; } = false;

        private ParallelOptions parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount };

        protected FileStreamProviders.IStreamProvider StreamProvider { get; set; }

        public string TableName { get; set; }

        private DataSet dataSet = null;

        private bool initialized = false;

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
            var typeDict = new Dictionary<TypeEnum, Type>() {
                {TypeEnum.None, typeof(string) },
                {TypeEnum.String, typeof(string) },
                {TypeEnum.DateTime, typeof(DateTime) },
                {TypeEnum.Boolean, typeof(bool) },
                {TypeEnum.Guid, typeof(Guid) },
                {TypeEnum.Int, typeof(int) },
                {TypeEnum.Long, typeof(long) },
                {TypeEnum.Decimal, typeof(decimal) },
                {TypeEnum.Double, typeof(double) }
            };

            #region read csv
            DataColumn[] cols;
            List<string[]> vals;

            var initTime = System.Diagnostics.Stopwatch.StartNew();

            using (var streamReader = new StreamReader(this.StreamProvider.GetStream()))
            {
                var header = streamReader.ReadLine();
                if (string.IsNullOrEmpty(header)) { throw new ArgumentException("The specified file is empty"); }
                cols = header.Split(this.ColSeperator).Select(o => new DataColumn(o)).ToArray();
                vals = new List<string[]>();
                while(!streamReader.EndOfStream) { vals.Add(streamReader.ReadLine().Split(this.ColSeperator)); }
            }

            // process read values

            Func<string, TypeEnum> getBestTypeForValue = v =>
            {
                if (string.IsNullOrEmpty(v)) { return TypeEnum.None; }
                if (bool.TryParse(v, out bool b) || this.CustomBooleanValues.ContainsKey(v.ToLower())) { return TypeEnum.Boolean; }
                if (DateTime.TryParse(v, out DateTime dat)) { return TypeEnum.DateTime; }
                if (Guid.TryParse(v, out Guid g)) { return TypeEnum.Guid; }
                if (int.TryParse(v, out int integer)) { return TypeEnum.Int; }
                if (long.TryParse(v, out long le)) { return TypeEnum.Long; }
                if (decimal.TryParse(v, out decimal dc)) { return TypeEnum.Decimal; }
                if (double.TryParse(v, out double p)) { return TypeEnum.Double; }
                return TypeEnum.String;
            };

            // apply column types
            Parallel.For(0, cols.Count(), this.parallelOptions, i =>
            {
                var t = vals.Max(o => getBestTypeForValue(o[i]));
                cols[i].DataType = typeDict[t];
                cols[i].AllowDBNull = this.ForceAllFieldsToAllowNull || vals.Any(o => string.IsNullOrEmpty(o[i]));
                if (t == TypeEnum.String)
                {
                    var l = vals.Max(o => o[i].Length);
                    l = l > 0 ? l + this.LengthTolerance : this.MaxFieldLength;
                    l = l < this.MaxFieldLength ? l : this.MaxFieldLength;
                    cols[i].MaxLength = l;
                }
                else if (t == TypeEnum.None)
                {
                    cols[i].MaxLength = this.MaxFieldLength;
                }
            });

            var dt = new DataTable(this.TableName);
            dt.Columns.AddRange(cols);

            for(var i = 0; i < vals.Count(); i++)
            {
                var va = vals.ElementAt(i);

                DataRow dr = null;
                lock(dt)
                {
                    dr = dt.NewRow();
                }

                Action<object, int> assign = (val, k) => { lock (dt) { dr[k] = val; } };
                
                for(var j = 0; j < cols.Length; j++)
                {
                    var dc = dt.Columns[j];
                    var v = va[j];
                    if (string.IsNullOrEmpty(v))
                    {
                        continue;
                    }
                    if (dc.DataType == typeof(string))
                    {
                        assign(v, j);
                        continue;
                    }
                    if (dc.DataType == typeof(int))
                    {
                        assign(int.Parse(v), j);
                        continue;
                    }
                    if (dc.DataType == typeof(long))
                    {
                        assign(long.Parse(v), j);
                        continue;
                    }
                    if (dc.DataType == typeof(decimal))
                    {
                        assign(decimal.Parse(v), j);
                        continue;
                    }
                    if (dc.DataType == typeof(double))
                    {
                        assign(double.Parse(v), j);
                        continue;
                    }
                    if (dc.DataType == typeof(DateTime))
                    {
                        assign(DateTime.Parse(v), j);
                        continue;
                    }
                    if (dc.DataType == typeof(bool))
                    {
                        if (bool.TryParse(v, out bool b)) { assign(b, j); }
                        else { assign(this.CustomBooleanValues[v.ToLower()], j); }
                        continue;
                    }
                    if (dc.DataType == typeof(Guid))
                    {
                        assign(Guid.Parse(v), j);
                        continue;
                    }
                }

                lock(dt)
                {
                    dt.Rows.Add(dr);
                }
            }

            var ds = new DataSet();
            ds.Tables.Add(dt);
            this.dataSet = ds;
            this.initialized = true;


            initTime.Stop();

            #endregion read csv

            this.InitProcessTime = initTime.Elapsed;
        }

        public void Dispose()
        {
            this.dataSet?.Dispose();
            this.dataSet = null;
            this.initialized = false;
        }

        public CsvDataSetProvider(FileStreamProviders.IStreamProvider streamProvider, string tableName)
        {
            this.TableName = tableName;
            this.StreamProvider = streamProvider;
        }
    }
}
