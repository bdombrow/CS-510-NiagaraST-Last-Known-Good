/* $Id */
package niagara.query_engine;

import java.util.ArrayList;

import org.w3c.dom.Node;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

import niagara.logical.VarToConstComparison;
import niagara.logical.Variable;
import niagara.logical.Constant;

/** Start with a tuple attribute, unnest a path, and compare the resulting
 * values with a constant - succeed if even one comparison succeeds. */
public class PathToConstComparisonImpl extends ComparisonImpl {
    private AtomicEvaluator leftAV;
    private String rightValue;
    private double sel;
    
    private ArrayList leftValues;

    public PathToConstComparisonImpl(VarToConstComparison pred) {
        super(pred.getOperator());
        Variable left = (Variable) pred.getLeft();
        Constant right = (Constant) pred.getRight();
        sel = pred.selectivity();
        
        rightValue = right.getValue();
        leftAV = left.getEvaluator(pred.getPath());
        leftValues = new ArrayList();
    }

    public boolean evaluate(StreamTupleElement t1, StreamTupleElement t2) {
        // Get the vector of atomic values to be compared
        leftValues.clear();

        leftAV.getAtomicValues(t1, t2, leftValues);

        // Loop over every combination of values and check whether
        // predicate holds
        int numLeft = leftValues.size();

        for (int left = 0; left < numLeft; ++left) {
            if (compareAtomicValues((String) leftValues.get(left), rightValue))
                return true;
        }

        // The comparison failed - return false
        return false;
    }

    public boolean evaluate(Node n) {
        // Get the vector of atomic values to be compared
        leftValues.clear();

        leftAV.getAtomicValues(n, leftValues);

        // Loop over every combination of values and check whether
        // predicate holds
        //
        int numLeft = leftValues.size();

        for (int left = 0; left < numLeft; ++left) {
            if (compareAtomicValues((String) leftValues.get(left), rightValue))
                return true;
        }

        // The comparison failed - return false
        return false;

    }

    public void resolveVariables(TupleSchema ts, int streamId) {
        leftAV.resolveVariables(ts, streamId);
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
