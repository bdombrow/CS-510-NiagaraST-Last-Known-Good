package niagara.physical.predicates;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

public class AndImpl implements PredicateImpl {
	private PredicateImpl left;
	private PredicateImpl right;

	public AndImpl(PredicateImpl left, PredicateImpl right) {
		this.left = left;
		this.right = right;
	}

	public boolean evaluate(Tuple t1, Tuple t2) {
		return left.evaluate(t1, t2) && right.evaluate(t1, t2);
	}

	public boolean evaluate(Node n) {
		return left.evaluate(n) && right.evaluate(n);
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
