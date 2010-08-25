namespace ContractsPrototype
{
    using System;
    using System.Collections.Generic;

    public class Scheme
    {
        // Fields
        private List<Template> _templates;
        private bool _isAny; // used for the "any" pattern in Feedback schemes

        // Properties
        public List<Template> Templates { get { _templates.Sort(); return _templates; } }
        public bool IsAny { get { return _isAny; } }

        // Ctor
        public Scheme()
        {
            _templates = new List<Template>();
            _isAny = false;
        }

        public static Scheme SchemeAsAny()
        {
            Scheme s = new Scheme();
            s._isAny = true;
            return s;
        }

        // Methods
        /// <summary>
        /// Add a template to the scheme
        /// </summary>
        /// <param name="t"></param>
        public void Add(Template t)
        {
            if (_isAny) { throw new Exception("Can't add templates to an \"Any\" scheme"); }
            _templates.Add(t);
        }

        /// <summary>
        /// Matches, assuming this is the subsequent.
        /// </summary>
        /// <param name="antecedent"></param>
        /// <returns></returns>
        public Boolean match(Scheme antecedent)
        {
            if (antecedent.IsAny)
                return true;
            else
            {
                if (this.IsAny)
                {
                    return false;
                }
                else
                {
                    if (this.Templates.Count != antecedent.Templates.Count)
                    {
                        return false;
                    }
                    else
                    {
                        Boolean matches = true;
                        for (int i = 0; i < this.Templates.Count; i++)
                        {
                            matches = matches && this.Templates[i].Equals(antecedent.Templates[i]);
                        }
                        return matches;
                    }
                }
            }
        }

        /// <summary>
        /// Pretty-print scheme
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (_isAny)
            {
                return "↺";
            }
            else if (_templates.Count == 0)
            {
                return "∅";
            }
            else
            {
                string _representation = "{";
                foreach (Template t in _templates)
                {
                    _representation = _representation + t.ToString() + ", ";
                }
                    return _representation.Substring(0, _representation.Length - 2) + "}";
            }
        }

    }
}
