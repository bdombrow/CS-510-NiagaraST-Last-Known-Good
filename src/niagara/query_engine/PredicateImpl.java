/* $Id: PredicateImpl.java,v 1.2 2002/12/10 01:17:45 vpapad Exp $ */
package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import org.w3c.dom.Node;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.utils.StreamTupleElement;

/** Implementation of a predicate */
public interface PredicateImpl {
    /** Determine whether these tuples satisfy the predicate */    
    boolean evaluate(StreamTupleElement t1, StreamTupleElement t2);
    
    /** Determine whether this value satisfies the predicate  */
    boolean evaluate(Node n);
    
    /** Resolve referenced variables to stream and attribute IDs */
    void resolveVariables(TupleSchema ts, int streamId);
    
    /** Selectivity of the corresponding predicate */
    double selectivity();
    
    /** Estimate the cost for evaluating this predicate (per tuple) */
    Cost getCost(ICatalog catalog);
}
