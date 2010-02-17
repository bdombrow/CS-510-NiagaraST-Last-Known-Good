package niagara.physical.predicates;

import java.util.ArrayList;

import niagara.logical.Variable;
import niagara.logical.predicates.ConstToVarComparison;
import niagara.logical.predicates.Constant;
import niagara.optimizer.colombia.Cost;
import niagara.optimizer.colombia.ICatalog;
import niagara.physical.AtomicEvaluator;
import niagara.physical.SimpleAtomicEvaluator;
import niagara.query_engine.TupleSchema;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

@SuppressWarnings("unchecked")
public class ConstToVarComparisonImpl extends ComparisonImpl {
	private AtomicEvaluator rightAV;
	private String leftValue;
	private double sel;

	private ArrayList rightValues;

	public ConstToVarComparisonImpl(ConstToVarComparison pred) {
		super(pred.getOperator());
		leftValue = ((Constant) pred.getLeft()).getValue();
		rightAV = ((Variable) pred.getRight()).getEvaluator();
		sel = pred.selectivity();
		rightValues = new ArrayList();
	}

	public boolean evaluate(Tuple t1, Tuple t2) {
		// Get the vector of atomic values to be compared
		rightValues.clear();

		rightAV.getAtomicValues(t1, t2, rightValues);

		// Loop over every combination of values and check whether
		// predicate holds
		//
		int numRight = rightValues.size();

		for (int right = 0; right < numRight; ++right) {
			if (compareAtomicValues(leftValue, (String) rightValues.get(right)))
				return true;
		}
		// The comparison failed
		return false;
	}

	public boolean evaluate(Node n) {
		String av = SimpleAtomicEvaluator.getAtomicValue(n);
		return compareAtomicValues(leftValue, av);
	}

	public void resolveVariables(TupleSchema ts, int streamId) {
		rightAV.resolveVariables(ts, streamId);
	}

	/**
	 * @see niagara.query_engine.PredicateImpl#getCost(ICatalog)
	 */
	public Cost getCost(ICatalog catalog) {
		// XXX vpapad: just use one blanket predicate cost for now
		return new Cost(catalog.getDouble("predicate_cost"));
	}

	/**
	 * @see niagara.query_engine.PredicateImpl#selectivity()
	 */
	public double selectivity() {
		return sel;
	}

}
