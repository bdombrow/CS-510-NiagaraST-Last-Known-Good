package niagara.physical;

import java.util.ArrayList;

import niagara.logical.Aggregate;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalOp;
import niagara.utils.BaseAttr;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

/**
 * This is the <code>PhysicalSumOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of Sum (a form of
 * grouping)
 * 
 * @version 1.0
 * 
 */
@SuppressWarnings("unchecked")
public abstract class PhysicalAggregate extends PhysicalGroup {

	Attribute aggrAttr;
	AtomicEvaluator ae;
	ArrayList atomicValues;

	protected abstract PhysicalAggregate getInstance();

	/*
	 * protected abstract Node constructAggrResult(AggrResult partialResult,
	 * AggrResult finalResult);
	 */
	protected abstract BaseAttr constructAggrResult(AggrResult partialResult,
			AggrResult finalResult);

	/*
	 * protected abstract void updateAggrResult(AggrResult result, Object
	 * ungroupedResult);
	 */
	protected abstract void updateAggrResult(AggrResult result,
			BaseAttr ungroupedResult);

	protected void localInitFrom(LogicalOp logicalOperator) {
		aggrAttr = ((Aggregate) logicalOperator).getAggrAttr();
	}

	protected void initializeForExecution() {
		ae = new AtomicEvaluator(aggrAttr.getName());
		ae.resolveVariables(inputTupleSchemas[0], 0);
		atomicValues = new ArrayList();
	}

	protected final BaseAttr getValue(Tuple tupleElement) {
		// First get the atomic values
		atomicValues.clear();
		ae.getAtomicValues(tupleElement, atomicValues);

		assert atomicValues.size() <= 1 : "Need exactly one atomic value";
		if (atomicValues.size() == 0)
			return null;

		return (BaseAttr) atomicValues.get(0);
	}

	protected final Double getDoubleValue(Tuple tupleElement)
			throws ShutdownException {
		try {
			// First get the atomic values
			atomicValues.clear();
			ae.getAtomicValues(tupleElement, atomicValues);

			assert atomicValues.size() <= 1 : "Need exactly one atomic value";
			if (atomicValues.size() == 0)
				return null;

			// Try to convert to double
			return new Double((String) (atomicValues.get(0)));
		} catch (java.lang.NumberFormatException nfe) {
			throw new ShutdownException(nfe.getMessage());
		}
	}

	/**
	 * This function merges a grouped result with an ungrouped result
	 * 
	 * @param groupedResult
	 *            The grouped result that is to be modified (this can be null)
	 * @param ungroupedResult
	 *            The ungrouped result that is to be grouped with groupedResult
	 *            (this can never be null)
	 * 
	 * @return The new grouped result
	 */

	/*
	 * protected final Object mergeResults (Object groupedResult, Object
	 * ungroupedResult) {
	 */
	protected final Object mergeResults(Object groupedResult,
			BaseAttr ungroupedResult) {

		// Set up the final result - if the groupedResult is null, then
		// create holder for final result, else just use groupedResult
		AggrResult finalResult = null;
		if (groupedResult == null) {
			finalResult = new AggrResult();
		} else {
			finalResult = (AggrResult) groupedResult;
		}

		// Add effects of ungrouped result
		updateAggrResult(finalResult, ungroupedResult);
		return finalResult;
	}

	/*
	 * public Node constructResult (Object partialResult, Object finalResult) {
	 */
	public BaseAttr constructResult(Object partialResult, Object finalResult) {
		return constructAggrResult((AggrResult) partialResult,
				(AggrResult) finalResult);
	}

	protected PhysicalGroup localCopy() {
		PhysicalAggregate op = getInstance();
		op.aggrAttr = this.aggrAttr;
		return op;
	}

	protected boolean localEquals(Object o) {
		return aggrAttr.equals(((PhysicalAggregate) o).aggrAttr);
	}

	public int hashCode() {
		return groupAttributeList.hashCode() ^ aggrAttr.hashCode();
	}

	/*
	 * class to be used to hold and pass around an aggregate result a union of
	 * all values needed for numeric aggregates - wastes a bit of space, but
	 * saves casting and makes code simpler.
	 */

	protected class AggrResult {
		int count;
		BaseAttr value;
		// double doubleVal;
	}
}
