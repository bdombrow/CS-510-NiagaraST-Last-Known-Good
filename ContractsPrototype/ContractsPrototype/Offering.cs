namespace ContractsPrototype
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    public class Offering
    {
        // Fields
        private int _inputArity;
        private int _outputArity;
        private Scheme[] _input;
        private Scheme[] _output;
        private Scheme[] _feedbackInput;
        private Scheme[] _feedbackOutput;

        // Properties
        public int InputArity { get { return _inputArity; } }
        public int OutputArity { get { return _outputArity; } }
        public Scheme[] Input { get { return _input; } }
        public Scheme[] Output { get { return _output; } }
        public Scheme[] FeedbackInput { get { return _feedbackInput; } }
        public Scheme[] FeedbackOutput { get { return _feedbackOutput; } }

        // Ctor
        public Offering(int inputArity, int outputArity)
        {
            _inputArity = inputArity;
            _outputArity = outputArity;
            _input = new Scheme[_inputArity];
            _output = new Scheme[_outputArity];
            _feedbackInput = new Scheme[_outputArity];
            _feedbackOutput = new Scheme[_inputArity];
        }

        // Methods

        //Checks this offering against an antecedent's offering
        public Boolean match(Offering antecedent, int antecedentId)
        {
            return this.Input[antecedentId].match(antecedent.Output[0]) // input = output
                && this.FeedbackOutput[antecedentId].match(antecedent.FeedbackInput[0]); // feedback output = feedback input
        }

        public Boolean matchParent(Offering subsequent, int antecedentId)
        {
            return this.Output[0].match(subsequent.Input[antecedentId]) // input = output
                && subsequent.FeedbackOutput[antecedentId].match(FeedbackInput[0]); // feedback output = feedback input
        }

              

        public void addInputScheme(int inputID, Scheme s)
        {
            if (inputID < 0 || inputID > _inputArity) { throw new Exception("Wrong arity adding scheme. Minimum arity is 1."); }
            _input[inputID - 1] = s;
        }

        public void addOutputScheme(int inputID, Scheme s)
        {
            if (inputID < 0 || inputID > _inputArity) { throw new Exception("Wrong arity adding scheme. Minimum arity is 1."); }
            _output[inputID - 1] = s;
        }

        public void addFeedbackInputScheme(int inputID, Scheme s)
        {
            if (inputID < 0 || inputID > _inputArity) { throw new Exception("Wrong arity adding scheme. Minimum arity is 1."); }
            _feedbackInput[inputID - 1] = s;
        }

        public void addFeedbackOutputScheme(int inputID, Scheme s)
        {
            if (inputID < 0 || inputID > _inputArity) { throw new Exception("Wrong arity adding scheme. Minimum arity is 1."); }
            _feedbackOutput[inputID - 1] = s;
        }

        /// <summary>
        /// Pretty-prints a contract offering.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string _representation = "<";
            int i;

            for (i = 0; i < _inputArity; i++)
            {
                _representation = _representation + "In" + (i + 1) + "=" + _input[i].ToString() + ", ";
            }

            for (i = 0; i < _outputArity; i++)
            {
                _representation = _representation + "Out" + (i + 1) + "=" + _output[i].ToString() + ", ";
            }

            for (i = 0; i < _outputArity; i++)
            {
                _representation = _representation + "Feedback.In" + (i + 1) + "=" + _feedbackInput[i].ToString() + ", ";
            }

            for (i = 0; i < _inputArity; i++)
            {
                _representation = _representation + "Feedback.Out" + (i + 1) + "=" + _feedbackOutput[i].ToString() + ", ";
            }

            return _representation.Substring(0, _representation.Length - 2) + ">";

        }

    }
}
