/* $Id: AndImpl.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

public class AndImpl implements PredicateImpl {
    private PredicateImpl left;
    private PredicateImpl right;

    public AndImpl(PredicateImpl left, PredicateImpl right) {
        this.left = left;
        this.right = right;
    }

    public boolean evaluate(StreamTupleElement t1, StreamTupleElement t2) {
        return left.evaluate(t1, t2) && right.evaluate(t1, t2);
    }

    public void resolveVariables(TupleSchema ts, int streamId) {
        left.resolveVariables(ts, streamId);
        right.resolveVariables(ts, streamId);
    }
    
    /**
     * @see niagara.query_engine.PredicateImpl#getCost(ICatalog)
     */
    public Cost getCost(ICatalog catalog) {
        Cost c = left.getCost(catalog);
        c.add(right.getCost(catalog).times((left.selectivity())));
        return c;
    }

    /**
     * @see niagara.query_engine.PredicateImpl#selectivity()
     */
    public double selectivity() {
        // Independence assumption
        return left.selectivity() * right.selectivity();
    }
}
