/* $Id$ */
package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import niagara.logical.*;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.PEException;
import niagara.utils.StreamTupleElement;
import niagara.xmlql_parser.syntax_tree.opType;

/** A comparison between two variables */
public class VarToVarComparisonImpl extends ComparisonImpl {

    private AtomicEvaluator leftAV, rightAV;

    private ArrayList leftValues;
    private ArrayList rightValues;
    private double sel;

    public VarToVarComparisonImpl(VarToVarComparison pred) {
        super(pred.getOperator());
        Variable left = (Variable) pred.getLeft();
        Variable right = (Variable) pred.getRight();
        sel = pred.selectivity();
        
        leftAV = left.getEvaluator();
        rightAV = right.getEvaluator();

        leftValues = new ArrayList();
        rightValues = new ArrayList();
    }

    /**
     * This function evaluate an arithmetic predicate on a tuple
     *
     * @param tuples The tuples on which the predicate is to be evaluated
     * @param arithmeticPred The arithmetic predicate to be evaluated
     *
     * @return True if the predicate is satisfied and false otherwise
     */

    public boolean evaluate(StreamTupleElement t1, StreamTupleElement t2) {
        // Get the vector of atomic values to be compared
        leftValues.clear();
        rightValues.clear();

        leftAV.getAtomicValues(t1, t2, leftValues);
        rightAV.getAtomicValues(t1, t2, rightValues);

        // Loop over every combination of values and check whether
        // predicate holds
        //
        int numLeft = leftValues.size();
        int numRight = rightValues.size();

        for (int left = 0; left < numLeft; ++left) {
            for (int right = 0; right < numRight; ++right) {
                if (compareAtomicValues((String) leftValues.get(left),
                    (String) rightValues.get(right))) {
                    // The comparison succeeds - return true
                    return true;
                }
            }
        }

        // The comparison failed - return false
        return false;
    }

    public void resolveVariables(TupleSchema ts, int streamId) {
        leftAV.resolveVariables(ts, streamId);
        rightAV.resolveVariables(ts, streamId);
    }
    /**
     * @see niagara.query_engine.PredicateImpl#getCost(ICatalog)
     */
    public Cost getCost(ICatalog catalog) {
        // XXX vpapad: just use one blanket predicate cost for now
        return new Cost(catalog.getDouble("predicate_cost"));
    }
    /**
     * @see niagara.query_engine.PredicateImpl#selectivity()
     */
    public double selectivity() {
        return sel;
    }
}