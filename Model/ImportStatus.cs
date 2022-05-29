using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatawarehouseCrawler.Model
{
    public enum StatusEnum { None = 0, Initialized = 1, Idle = 2, OK = 3, Warning = 4, Error = 5 }

    public enum KeyIntegrityStatusEnum { None = 0, Idle = 1, Ignored = 2, OK = 3, DoubleKeys = 4, Error = 5 }

    public enum ProgressStatusEnum { None = 0, Idle = 1 }

    public enum ApplyFunctionEnum { none = 0, flushdata = 1, recreatetable = 2 }

    public class ImportStatus
    {
        public string Name { get; set; }

        public string Connection { get; set; }
        
        public int Status { get; set; }

        public StatusEnum StatusEnum { get { return (StatusEnum)this.Status; } set { this.Status = (int)value; } }

        public int KeyIntegrityStatus { get; set; }

        public int LastKeyIntegrityStatus { get; set; }

        public int ConsitencyFastFixStatus { get; set; }

        public int LastConsitencyFastFixStatus { get; set; }

        public int ConsitencyFixStatus { get; set; }

        public int LastConsitencyFixStatus { get; set; }

        public KeyIntegrityStatusEnum KeyIntegrityStatusEnum { get { return (KeyIntegrityStatusEnum)this.KeyIntegrityStatus; } set { this.KeyIntegrityStatus = (int)value; } }

        public int SourceCount { get; set; }

        public int TargetCount { get; set; }

        public int RowsInserted { get; set; }

        public int RowsUpdated { get; set; }

        public int CorrectionRowsInserted { get; set; }

        public int CorrectionRowsDeleted { get; set; }

        public int TotalRowsInserted { get; set; }

        public int TotalRowsDifference { get; set; }

        public int LastCorrectionRowsInserted { get; set; }

        public int LastCorrectionRowsDeleted { get; set; }

        public int LastTotalRowsInserted { get; set; }

        public int LastTotalRowsDifference { get; set; }

        public int Progress { get; set; }

        public int ProgressStatus { get; set; }

        public ProgressStatusEnum ProgressStatusEnum { get { return (ProgressStatusEnum)this.ProgressStatus; } set { this.ProgressStatus = (int)value; } }

        public int ProgressedRows { get; set; }

        public DateTime ProgressModified { get; set; }

        public int ApplyFunction { get; set; }
        public ApplyFunctionEnum ApplyFunctionEnum { get { return (ApplyFunctionEnum)this.ApplyFunction; } set { this.ApplyFunction = (int)value; } }

        public string ErrorMessage { get; set; }

        public long Duration { get; set; }

        public long ConsistencyFixDuration { get; set; }

        public long ConsistencyFastFixDuration { get; set; }

        public TimeSpan DurationTime { get { return new TimeSpan(Duration); } set { this.Duration = value.Ticks; } }

        public TimeSpan ConsistencyFixDurationTime { get { return new TimeSpan(ConsistencyFixDuration); } set { this.ConsistencyFixDuration = value.Ticks; } }

        public TimeSpan ConsistencyFastFixDurationTime { get { return new TimeSpan(ConsistencyFastFixDuration); } set { this.ConsistencyFastFixDuration = value.Ticks; } }

        public DateTime Modified { get; set; }

        public int LastStatus { get; set; }

        public StatusEnum LastStatusEnum { get { return (StatusEnum)this.LastStatus; } set { this.LastStatus = (int)value; } }

        public int LastSourceCount { get; set; }
        public int LastTargetCount { get; set; }
        public int LastRowsInserted { get; set; }
        public int LastRowsUpdated { get; set; }
        public long LastDuration { get; set; }
        public long LastConsistencyFixDuration { get; set; }
        public long LastConsistencyFastFixDuration { get; set; }
        public TimeSpan LastDurationTime { get { return new TimeSpan(LastDuration); } set { this.LastDuration = value.Ticks; } }
        public string LastErrorMessage { get; set; }
        public DateTime? LastModified { get; set; }

        public override string ToString()
        {
            return this.Name;
        }
    }
}
