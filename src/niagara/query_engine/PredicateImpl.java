/* $Id: PredicateImpl.java,v 1.1 2002/10/06 23:56:41 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

/** Implementation of a predicate */
public interface PredicateImpl {
    /** Determine whether these tuples satisfy the predicate */    
    boolean evaluate(StreamTupleElement t1, StreamTupleElement t2);
    
    /** Resolve referenced variables to stream and attribute IDs */
    void resolveVariables(TupleSchema ts, int streamId);
    
    /** Selectivity of the corresponding predicate */
    double selectivity();
    
    /** Estimate the cost for evaluating this predicate (per tuple) */
    Cost getCost(ICatalog catalog);
}
