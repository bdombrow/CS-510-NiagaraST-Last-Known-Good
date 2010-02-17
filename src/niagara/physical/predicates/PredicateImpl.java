package niagara.physical.predicates;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

/** Implementation of a predicate */
public interface PredicateImpl {
	/** Determine whether these tuples satisfy the predicate */
	boolean evaluate(Tuple t1, Tuple t2);

	/** Determine whether this value satisfies the predicate */
	boolean evaluate(Node n);

	/** Resolve referenced variables to stream and attribute IDs */
	void resolveVariables(TupleSchema ts, int streamId);

	/** Selectivity of the corresponding predicate */
	double selectivity();

	/** Estimate the cost for evaluating this predicate (per tuple) */
	Cost getCost(ICatalog catalog);
}
