namespace ContractsPrototype
{
    using System;
    using System.Collections.Generic;

    public class Template : IComparable
    {
        #region Fields
        private Dictionary<String, ClassType.mark> _template;
        private List<String> _attributes;
        #endregion

        #region Properties
        public Dictionary<String, ClassType.mark> Scheme { get { return _template; } }
        public List<String> Attributes { get { _attributes.Sort();  return _attributes; } }
        #endregion

        #region Ctor
        public Template()
        {
            _template = new Dictionary<string, ClassType.mark>();
            _attributes = new List<string>();
        }
        #endregion

        // Methods

        /// <summary>
        /// Add an attribute and a mark to the template
        /// </summary>
        /// <param name="attribute">Attribute name</param>
        /// <param name="mark">Marker</param>
        public void Add(String attribute, ClassType.mark mark)
        {
            _template.Add(attribute, mark);
            _attributes.Add(attribute);
        }

        /// <summary>
        /// Pretty-prints the template
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string _representation = "[[";

            foreach (string a in Attributes)
            {
                _representation = _representation + a + ":" + ClassType.ToString(_template[a]) + ", ";
            }
            return _representation.Substring(0, _representation.Length - 2) + "]]";
        }

        public override bool Equals(object obj)
        {
            if (obj is Template)
            {
                return this.ToString().Equals(((Template)obj).ToString());
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }

        #region IComparable Members

        public int CompareTo(object obj)
        {
            if (!(obj is Template))
            {
                return -1;
            }
            else
            {
                Template other = (Template)obj;
                return this.ToString().CompareTo(other.ToString());
            }
        }

        #endregion
    }
}
