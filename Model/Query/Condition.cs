using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace DatawarehouseCrawler.Model.Query
{
    public class Condition:ICloneable
    {
        #region static methods

        public static Condition Combine(IEnumerable<IEnumerable<Condition>> conditions, ConnectSymbols connect = ConnectSymbols.and, ConnectSymbols subConnect = ConnectSymbols.and, Action<Condition> each = null)
        {
            if (conditions == null) { return null; }
            Condition ret = null, current = null;
            foreach (var cl in conditions)
            {
                var innerCombine = Combine(cl, subConnect, each);
                // connect to current element
                if (current != null)
                {
                    current.ConnectCondition = innerCombine;
                    current.ConnectSymbol = connect;
                }
                // select first for return
                else
                {
                    ret = innerCombine;
                }
                
                current = innerCombine.Last();
            }

            return ret;
        }

        public static Condition Combine(IEnumerable<Condition> conditions, ConnectSymbols connect = ConnectSymbols.and, Action<Condition> each = null)
        {
            if (conditions == null) { return null; }
            Condition ret = null, current = null;
            foreach (var c in conditions)
            {
                Condition co = c?.Clone() as Condition;
                if (current != null)
                {
                    current.ConnectCondition = co;
                    current.ConnectSymbol = connect;
                }
                // select first for return
                else
                {
                    ret = co;
                }

                each?.Invoke(co);
                current = co?.Last();
            }

            return ret;
        }

        public static Condition Combine(params Condition[] conditions) { return Combine(conditions?.AsEnumerable()); }

        //public static Condition Combine(params IEnumerable<Condition>[] conditions) { return conditions.Select(o=>Combine(oCombine(conditions); }

        #endregion static methods

        public Column Column { get; set; }
        public CompareSymbol CompareSymbol { get; set; }
        public object Value { get; set; }
        public ConnectSymbols ConnectSymbol { get; set; }
        public Condition ConnectCondition { get; set; }
        public uint Priority { get; set; }
        public Condition() { }
        public Condition Each(Action<Condition> each)
        {
            Condition cond = this, next = null;
            var i = 0;
            do
            {
                if (i > 0) { cond = next; }
                each?.Invoke(cond);
                next = cond.ConnectCondition;
                i++;
            }
            while (next != null);
            return this;
        }
        public Condition Each(Action<Condition, int> each)
        {
            var i = 0;
            this.Each(c => { each?.Invoke(c, i); i++; });
            return this;
        }

        public object Clone()
        {
            return new Condition()
            {
                Column = this.Column,
                CompareSymbol = this.CompareSymbol,
                Value = this.Value,
                ConnectSymbol = this.ConnectSymbol,
                Priority = this.Priority,
                ConnectCondition = (this.ConnectCondition != null ? this.ConnectCondition.Clone() : null) as Condition
            };
        }

        public List<Condition> ToList()
        {
            var ret = new List<Condition>();
            this.Each((c, i) => ret.Add(c));
            return ret;
        }

        public List<Condition> ToCloneList()
        {
            return ((Condition)this.Clone()).ToList();
        }

        public IEnumerable<object> GetValues()
        {
            return this.ToList().Select(o => o.Value);
        }

        public IEnumerable<string> GetValuesAsString()
        {
            return this.ToList().Select(o => o.Value?.ToString()); 
        }

        public IEnumerable<string> GetSqlTypesAsString()
        {
            return this.ToList().Select(o => o.Column?.SqlType.ToString("f"));
        }

        public Condition Last()
        {
            var current = this;
            while(current != null)
            {
                if (current.ConnectCondition == null) return current;
                else current = current.ConnectCondition;   
            }

            return null;
        }

        //public Condition(Condition copy) : this(copy.Column, copy.CompareSymbol, copy.Value, copy.ConnectCondition) {
        //    this.Priority = copy.Priority;
        //}

        public Condition(Column column, CompareSymbol compareSymbol, object value, Condition subCondition = null)
        {
            this.Column = column; this.CompareSymbol = compareSymbol; this.Value = value; this.ConnectCondition = subCondition;
        }
    }

    public class JoinCondition : Condition
    {
        public Column JoinColumn
        {
            get
            {
                return this.Value as Column;
            }
            set
            {
                this.Value = value;
            }
        }
    }
}