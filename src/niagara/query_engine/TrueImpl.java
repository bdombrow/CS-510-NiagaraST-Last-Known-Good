/* $Id: TrueImpl.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

import java.util.HashMap;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

public class TrueImpl implements PredicateImpl {
    /** There is only one truth! */
    private static final TrueImpl trueImpl = new TrueImpl();
    
    /** TrueImpl is a singleton */
    private TrueImpl() {};
    
    public static TrueImpl getTrueImpl() {
        return trueImpl;
    }
    
    public boolean evaluate(StreamTupleElement t1, StreamTupleElement t2) {
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
