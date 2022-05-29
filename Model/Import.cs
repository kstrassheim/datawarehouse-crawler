using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.Model
{
    public enum TypeEnum { none = 0, fact = 1, dim = 2 }

    public enum SourceTypeEnum { sql = 0, odata = 1, dataset = 2 }

    public enum FileTypeEnum { none=0, excelxml=1, csv=2}
    
    public enum StreamTypeEnum { none = 0, azurestorage = 1, ftp = 2 }

    public enum DataSyncTypeEnum { none = 0, appendbyid = 1, appendbydate = 2, appendbyidexclude = 3, appendbyidfirst = 4, forcedeleteexisting = 5, appendbydatestrict = 6 }

    public enum ExpectedSizeEnum { none = 0, verylarge = 1, large = 2, big = 3, medium = 4, small = 5 }

    public enum UpdateModeEnum { none = 0, updateByModifiedDate = 1, forceupdateall = 2 }

    public class ImportModel
    {
        public string Name { get; set; }

        public string Connection { get; set; }

        public string SourceName { get; set; }

        public string IdFieldName { get; set; }

        public string InsertQueryDateFieldName { get; set; }

        public string InsertQueryDateFormat { get; set; }

        public string DefaultQueryParamsSuffix { get; set; }

        public string UpdateQueryDateFieldName { get; set; }

        public string IgnoreUpdateColumns { get; set; }

        public string UpdateQueryDateFormat { get; set; }

        public string UpdateMode { get; set; }

        public UpdateModeEnum UpdateModeEnum { get { return (UpdateModeEnum)Enum.Parse(typeof(UpdateModeEnum), this.UpdateMode ?? UpdateModeEnum.none.ToString("D")); } set { this.UpdateMode = value.ToString("D"); } }

        public string Type { get; set; }

        public TypeEnum TypeEnum { get { return (TypeEnum)Enum.Parse(typeof(TypeEnum), this.Type ?? TypeEnum.none.ToString("D")); } set { this.Type = value.ToString("D"); } }

        public string DataSyncType { get; set; }

        public DataSyncTypeEnum DataSyncTypeEnum { get { return (DataSyncTypeEnum)Enum.Parse(typeof(DataSyncTypeEnum), this.DataSyncType ?? DataSyncTypeEnum.none.ToString("D")); } set { this.DataSyncType = value.ToString("D"); } }

        public string ExpectedSize { get; set; }

        public ExpectedSizeEnum ExpectedSizeEnum { get { return (ExpectedSizeEnum)Enum.Parse(typeof(ExpectedSizeEnum), this.ExpectedSize ?? ExpectedSizeEnum.none.ToString("D")); } set { this.ExpectedSize = value.ToString("D"); } }

        public string DistributionColumn { get; set; }

        public override string ToString() { return this.Name; }

        public string SourceType { get; set; }

        public SourceTypeEnum SourceTypeEnum { get { return (SourceTypeEnum)Enum.Parse(typeof(SourceTypeEnum), this.SourceType ?? SourceTypeEnum.sql.ToString("D")); } set { this.SourceType = value.ToString("D"); } }

        public string QuerySubUrl { get; set; }

        public int Pagesize { get; set; }

        public bool AzureDwhIgnoreIdentity { get; set; }

        public JoinModel[] Join { get; set; }

        public bool IgnoreKeyIntegrityCheck { get; set; }

        public bool IgnoreImportIfSourceIsNotAvailable { get; set; }

        public bool IgnoreCountConsistencyCheck { get; set; }

        public bool AvoidCountConsistencyCorrection { get; set; }

        public bool AvoidCompleteConsistencyCorrection { get; set; }
    }

    public class JoinModel
    {
        public string Name { get; set; }
        public string SourceName { get; set; }
        public string IdFieldName { get; set; }
        public string ParentJoinFieldName { get; set; }
        public string SelectFields { get; set; }
        public string ParentExtraInsertQueryDateFieldName { get; set; }
        public string ParentExtraUpdateQueryDateFieldName { get; set; }
    }
}
