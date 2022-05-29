using DatawarehouseCrawler.Model;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DatawarehouseCrawler.Model.Query
{
    public class Column
    {
        private readonly static Dictionary<Type, SqlDbType> typeMap = new Dictionary<Type, SqlDbType>() {
                        {typeof(string), SqlDbType.NVarChar },
                        {typeof(char[]), SqlDbType.NVarChar },
                        {typeof(int), SqlDbType.Int },
                        {typeof(Int16), SqlDbType.SmallInt },
                        {typeof(Int64),SqlDbType.BigInt },
                        {typeof(Byte[]),SqlDbType.VarBinary },
                        {typeof(Boolean),SqlDbType.Bit },
                        {typeof(DateTime),SqlDbType.DateTime2 },
                        {typeof(DateTimeOffset),SqlDbType.DateTimeOffset },
                        {typeof(Decimal),SqlDbType.Decimal },
                        {typeof(Double),SqlDbType.Float },
                        {typeof(Byte),SqlDbType.TinyInt },
                        {typeof(TimeSpan),SqlDbType.Time },
                        {typeof(Guid),SqlDbType.UniqueIdentifier }
                    };

        private readonly static SqlDbType[] SqlIntTypes = { SqlDbType.SmallInt, SqlDbType.Int, SqlDbType.TinyInt, SqlDbType.BigInt, SqlDbType.Bit };
        private readonly static SqlDbType[] SqlNumericTypes = { SqlDbType.SmallInt, SqlDbType.Float, SqlDbType.Decimal, SqlDbType.Money, SqlDbType.SmallMoney, SqlDbType.Int, SqlDbType.TinyInt, SqlDbType.BigInt, SqlDbType.Bit };
        public readonly static SqlDbType[] SqlDateTypes = { SqlDbType.Date, SqlDbType.DateTime, SqlDbType.DateTime2 };

        public string InternalName { get; set; }
        public string Name { get; set; }
        public string Alias { get; set; }
       
        public Type Type { get; set; }
        public SqlDbType SqlType { 
            get { 
                if (this.OriginalColumnTypeInfo != null)
                {
                    try
                    {
                        var t = Enum.Parse<SqlDbType>(Enum.GetNames(typeof(SqlDbType)).First(o => o.ToLower() == this.OriginalColumnTypeInfo.TypeName.ToLower()));
                        return t;
                    }
                    catch { }
                }
                return typeMap[this.Type];
            } 
        }
        public bool IsSqlIntType { get { return SqlIntTypes.Contains(this.SqlType); } }

        public bool IsSqlNumericType { get { return SqlNumericTypes.Contains(this.SqlType); } }

        public bool IsSqlDateType { get { return SqlDateTypes.Contains(this.SqlType); } }

        public int Length { get; set; }
        public string TableAlias { get; set; }
        public bool Nullable { get; set; }
        public bool IsIdentity { get; set; }
        public bool IgnoreOnUpdate { get; set; }

        public ColumnTypeInfo OriginalColumnTypeInfo { get; set; }

        public override int GetHashCode()
        {
            var hc = !string.IsNullOrEmpty(this.Name) ? this.Name.GetHashCode() : 0;
            hc += !string.IsNullOrEmpty(this.Alias) ? this.Alias.GetHashCode() : 0;
            hc += !string.IsNullOrEmpty(this.TableAlias) ? this.TableAlias.GetHashCode() : 0;

            return hc;
        }

        public override string ToString()
        {
            return $"{(!string.IsNullOrEmpty(this.Alias)? $"{this.Alias}." : string.Empty)}{this.Name}";
        }
    }

    public class JoinedColumn : Column
    {
        public JoinCondition Condition;
        public JoinModel Model { get; set; }
    }

    public class CountRowsColumn :Column
    {
        public CountRowsColumn() { }
    }

    public class MaxValueColumn : Column
    {
        public MaxValueColumn() { }

        public MaxValueColumn(Column c)
        {
            this.InternalName = c.InternalName;
            this.Name = c.Name;
            this.Alias = c.Alias;
            this.Type = c.Type;
            this.Length = c.Length;
            this.TableAlias = c.TableAlias;
            this.Nullable = c.Nullable;
            this.IsIdentity = c.IsIdentity;
        }
    }

    public class JoinedMaxValueColumn : JoinedColumn
    {
        public JoinedMaxValueColumn() { }

        public JoinedMaxValueColumn(JoinedColumn c)
        {
            this.InternalName = c.InternalName;
            this.Name = c.Name;
            this.Alias = c.Alias;
            this.Type = c.Type;
            this.Length = c.Length;
            this.TableAlias = c.TableAlias;
            this.Nullable = c.Nullable;
            this.IsIdentity = c.IsIdentity;
            this.Condition = c.Condition;
            this.Model = c.Model;
        }
    }
}
