/* $Id: NotImpl.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

public class NotImpl implements PredicateImpl {
    private PredicateImpl p;

    public NotImpl(PredicateImpl p) {
        this.p = p;
    }

    public boolean evaluate(StreamTupleElement t1, StreamTupleElement t2) {
        return !p.evaluate(t1, t2);
    }

    public void resolveVariables(TupleSchema ts, int streamId) {
        p.resolveVariables(ts, streamId);
    }
    
    /**
     * @see niagara.query_engine.PredicateImpl#getCost(ICatalog)
     */
    public Cost getCost(ICatalog catalog) {
        return p.getCost(catalog);
    }
    
    /**
     * @see niagara.query_engine.PredicateImpl#selectivity()
     */
    public double selectivity() {
        return 1 - p.selectivity();
    }
}
