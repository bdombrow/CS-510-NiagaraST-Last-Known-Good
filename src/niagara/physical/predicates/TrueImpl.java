/* $Id: TrueImpl.java,v 1.1 2003/12/24 01:44:23 vpapad Exp $ */
package niagara.physical.predicates;


import org.w3c.dom.Node;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

public class TrueImpl implements PredicateImpl {
    /** There is only one truth! */
    private static final TrueImpl trueImpl = new TrueImpl();
    
    /** TrueImpl is a singleton */
    private TrueImpl() {};
    
    public static TrueImpl getTrueImpl() {
        return trueImpl;
    }
    
    public boolean evaluate(Tuple t1, Tuple t2) {
        return true;
    }
    
    public boolean evaluate(Node n) {
        return true;
    }
    
    public void resolveVariables(TupleSchema ts, int streamId) {}
    
    /**
     * @see niagara.query_engine.PredicateImpl#getCost(ICatalog)
     */
    public Cost getCost(ICatalog catalog) {
        return new Cost(0);
    }

    /**
     * @see niagara.query_engine.PredicateImpl#selectivity()
     */
    public double selectivity() {
        return 1;
    }

}
