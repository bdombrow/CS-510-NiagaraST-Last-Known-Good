/* $Id: FalseImpl.java,v 1.1 2003/12/24 01:44:23 vpapad Exp $ */
package niagara.physical.predicates;

import org.w3c.dom.Node;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

public class FalseImpl implements PredicateImpl {
    private static final FalseImpl falseImpl = new FalseImpl();
    
    /** FalseImpl is a singleton */
    private FalseImpl() {};
    
    public static FalseImpl getFalseImpl() {
        return falseImpl;
    }
    
    public boolean evaluate(Tuple t1, Tuple t2) {
        return false;
    }

     public boolean evaluate(Node n) {
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
