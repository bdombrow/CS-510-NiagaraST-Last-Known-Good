using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ContractsPrototype
{
    public class Plan : ICloneable
    {
        private List<Operator> _ops;

        public List<Operator> Operators { get { _ops.Sort(); return _ops; } }
        public Operator Top
        {
            get
            {
                foreach (Operator op in Operators)
                {
                    if (op.Subsequents[0] == null)
                        return op;
                }
                return null;
            }
        }
        

        public Plan()
        {
            _ops = new List<Operator>();
        }

        public void AddOperator(Operator op) {
            _ops.Add(op);
        }

        public Boolean HasOfferings()
        {
            Boolean result = true;
            foreach (Operator op in this.Operators)
            {
                result = result && (op.Offerings.Count > 0);
            }
            return result;
        }

        public override string ToString()
        {
            string _representation = null;
            foreach (Operator o in Operators)
            {
                _representation = _representation + o.ToString() + "\n";
            }
            return _representation;
        }

        public Plan ConsistentAccordance()
        {
            Plan consistent = (Plan)this.Clone();
            Operator top = consistent.Top;
            top.Consistent();
            return consistent;
        }

        #region ICloneable Members

        public object Clone()
        {
            return this.MemberwiseClone();
        }

        #endregion
    }
}
