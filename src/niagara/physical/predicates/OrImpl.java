/* $Id: OrImpl.java,v 1.1 2003/12/24 01:44:23 vpapad Exp $ */
package niagara.physical.predicates;

import org.w3c.dom.Node;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

/** Implementation of Or */
public class OrImpl implements PredicateImpl {
    private PredicateImpl left;
    private PredicateImpl right;

    public OrImpl(PredicateImpl left, PredicateImpl right) {
        this.left = left;
        this.right = right;
    }

    public boolean evaluate(Tuple t1, Tuple t2) {
        return left.evaluate(t1, t2) || right.evaluate(t1, t2);
    }

    public boolean evaluate(Node n) {
        return left.evaluate(n) || right.evaluate(n);
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
        c.add(right.getCost(catalog).times((1 - left.selectivity())));
        return c;
    }

    /**
     * @see niagara.query_engine.PredicateImpl#selectivity()
     */
    public double selectivity() {
        // Independence assumption
        double l = left.selectivity();
        double r = right.selectivity();
        return l + r - l*r;
    }
}
