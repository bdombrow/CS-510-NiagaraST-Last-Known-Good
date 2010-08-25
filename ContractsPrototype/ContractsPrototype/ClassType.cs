namespace ContractsPrototype
{
    using System;

    public class ClassType
    {
        /// <summary>
        /// Allowable marks in templates
        /// </summary>
        public enum mark { PLUS, HASH, MINUS }

        /// <summary>
        /// return the String representation of a mark
        /// </summary>
        /// <param name="c">mark</param>
        /// <returns>String</returns>
        public static string ToString(mark c) {
            switch (c)
            {
                case mark.PLUS: return "+";
                case mark.MINUS: return "-";
                case mark.HASH: return "#";
                default : throw new Exception("Unexpected mark type");
            }
        }

        public static mark ToMark(string c)
        {
            switch (c)
            {
                case "+": return mark.PLUS;
                case "-": return mark.MINUS;
                case "#": return mark.HASH;
                default: throw new Exception("Unexpected mark type");
            }
        }

    }
}
