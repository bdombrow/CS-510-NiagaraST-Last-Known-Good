namespace ContractsPrototype
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public class Operator : IComparable<Operator>
    {
        // Fields
        private string _name;
        private List<Offering> _offerings;
        private int _inputArity;
        private int _outputArity;
        private Operator[] _antecedents;
        private Operator[] _subsequents;


        // Properties
        public string Name { get { return _name; } }
        public List<Offering> Offerings { get { return _offerings; } }
        public int InputArity { get { return _inputArity; } }
        public int OutputArity { get { return _outputArity; } }
        public Operator[] Antecedents
        {
            get
            {
                if (_antecedents.Length == 1 && (_antecedents[0] == null))
                {
                    return null;
                }
                else return _antecedents;
            }
        }
        public Operator[] Subsequents { get { return _subsequents; } }


        // Ctor
        public Operator(string name, int inputArity, int outputArity)
        {
            _name = name;
            _inputArity = inputArity;
            _outputArity = outputArity;
            _offerings = new List<Offering>();
            _antecedents = new Operator[_inputArity];
            _subsequents = new Operator[_outputArity];
        }

        // Methods

        public void AddAntecedent(Operator op, int position)
        {
            _antecedents[position] = op;
            op.AddSubsequent(this);
        }

        public void AddSubsequent(Operator op)
        {
            _subsequents[0] = op;
        }

        public void AddOffering(Offering o)
        {
            _offerings.Add(o);
        }

        public override string ToString()
        {
            string _representation = _name + " = {";
            foreach (Offering o in _offerings)
            {
                _representation = _representation + o.ToString() + ", ";
            }
            return _representation.Substring(0, _representation.Length - 2) + "}";
        }

        #region IComparable<Operator> Members

        public int CompareTo(Operator other)
        {
            return Name.CompareTo(other.Name);
        }

        #endregion

        public void Consistent()
        {
            if (this.Antecedents != null)
            {
                for (int i = 0; i < this.Antecedents.Length; i++)
                {
                    match(this.Antecedents[i], i);
                    this.Antecedents[i].Consistent();
                    match(this.Antecedents[i], i);
                    //this.Antecedents[i].match(this, 0);
                }

                if (!(this.Subsequents[0] == null))
                {
                    // where am I in my parent?
                    int pos = 0;
                    for (int i = 0; i < this.Subsequents[0].Antecedents.Length; i++)
                    {
                        if (this.Subsequents[0].Antecedents[i].Name.Equals(this.Name))
                        {
                            pos = i;
                        }
                    }
                    this.Subsequents[0].match(this, pos);
                }
            }
        }

        // prunes offerings both ways
        public void match(Operator op2, int inputId)
        {

            // do this op first
            var OfferingsToRemove = new List<Offering>();

            foreach (Offering of1 in this.Offerings)
            {
                Boolean foundMatchingOffering = false;

                foreach (Offering of2 in op2.Offerings)
                    foundMatchingOffering = foundMatchingOffering || of1.match(of2, inputId);

                if (foundMatchingOffering == false)
                    OfferingsToRemove.Add(of1);
            }

            foreach (Offering of in OfferingsToRemove)
                this.Offerings.Remove(of);

            // now do op2
            if (op2 != null)
            {
                var OfferingsToRemove2 = new List<Offering>();
                foreach (Offering of2 in op2.Offerings)
                {
                    foreach (Offering of1 in this.Offerings)
                    {
                        if (of2.matchParent(of1, inputId) == false)
                            OfferingsToRemove2.Add(of2);
                    }
                }

                foreach (Offering of2 in OfferingsToRemove2)
                    op2.Offerings.Remove(of2);
            }

        }
    }
}