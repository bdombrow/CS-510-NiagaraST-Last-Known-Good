package niagara.physical.predicates;

import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

public class NotImpl implements PredicateImpl {
	private PredicateImpl p;

	public NotImpl(PredicateImpl p) {
		this.p = p;
	}

	public boolean evaluate(Tuple t1, Tuple t2) {
		return !p.evaluate(t1, t2);
	}

	public boolean evaluate(Node n) {
		return !p.evaluate(n);
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
