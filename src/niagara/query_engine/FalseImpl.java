/* $Id: FalseImpl.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

import java.util.HashMap;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

public class FalseImpl implements PredicateImpl {
    private static final FalseImpl falseImpl = new FalseImpl();
    
    /** FalseImpl is a singleton */
    private FalseImpl() {};
    
    public static FalseImpl getFalseImpl() {
        return falseImpl;
    }
    
    public boolean evaluate(StreamTupleElement t1, StreamTupleElement t2) {
        return false;
    }
    
    public void resolveVariables(TupleSchema ts, int streamId) {}

    public Cost getCost(ICatalog catalog) {
        return new Cost(0);
    }

    public double selectivity() {
        return 0;
    }
}