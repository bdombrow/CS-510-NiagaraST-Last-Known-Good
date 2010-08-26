package niagara.physical;

import java.util.ArrayList;
import java.util.Iterator;

import niagara.logical.WindowAggregate;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalOp;
import niagara.utils.BaseAttr;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.FeedbackType;
import niagara.utils.FeedbackPunctuation.Comparator;

/**
 * This is the <code>PhysicalSumOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of Sum (a form of
 * grouping)
 * 
 * @version 1.0
 * 
 */
@SuppressWarnings("unchecked")
public abstract class PhysicalWindowAggregate extends PhysicalWindowGroup {

	ArrayList<Attribute> aggrAttr = new ArrayList();
	ArrayList<AtomicEvaluator> ae = new ArrayList();
	ArrayList atomicValues;

	// Propagate
	// boolean propagate;

	protected abstract PhysicalWindowAggregate getInstance();

	protected abstract BaseAttr constructAggrResult(AggrResult partialResult,
			AggrResult finalResult);

	protected abstract void updateAggrResult(AggrResult result,
			Object ungroupedResult);

	protected void localInitFrom(LogicalOp logicalOperator) {
		// propagate = ((WindowAggregate)logicalOperator).getPropagate();
		aggrAttr = ((WindowAggregate) logicalOperator).getAggrAttr();
		widName = ((WindowAggregate) logicalOperator).getWid();
	}

	protected void initializeForExecution() {
		for (Attribute attr : aggrAttr) {
			ae.add(new AtomicEvaluator(attr.getName()));
			ae.get(ae.size() - 1).resolveVariables(inputTupleSchemas[0], 0);
		}
		atomicValues = new ArrayList();
	}

	/*
	 * protected final Double getDoubleValue (Tuple tupleElement) throws
	 * ShutdownException { try { // First get the atomic values
	 * atomicValues.clear(); ae.getAtomicValues(tupleElement, atomicValues);
	 * assert atomicValues.size() == 1 : "Need exactly one atomic value"; // Try
	 * to convert to double return new Double((String)(atomicValues.get(0))); }
	 * catch (java.lang.NumberFormatException nfe) { throw new
	 * ShutdownException(nfe.getMessage()); } }
	 */

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

	protected final Object mergeResults(Object groupedResult,
			Object ungroupedResult) {

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

	public BaseAttr constructResult(Object partialResult, Object finalResult) {
		return constructAggrResult((AggrResult) partialResult,
				(AggrResult) finalResult);
	}

	protected PhysicalWindowGroup localCopy() {
		PhysicalWindowAggregate op = getInstance();
		op.aggrAttr = this.aggrAttr;
		op.widName = widName;
		op.propagate = propagate;
		op.fAttr = fAttr;
		op.logging = logging;
		op.log = log;
		op.exploit = exploit;
		
		return op;
	}

	protected boolean localEquals(Object o) {
		return aggrAttr.equals(((PhysicalWindowAggregate) o).aggrAttr)
				&& widName.equals(((PhysicalWindowAggregate) o).widName); // &&
		// (propagate && ((PhysicalWindowAggregate)o).propagate);
	}

	public int hashCode() {
		// int i = 0;
		// if (propagate)
		// i = 1;
		return groupAttributeList.hashCode() ^ aggrAttr.hashCode()
				^ widName.hashCode();
	}

	/*
	 * class to be used to hold and pass around an aggregate result a union of
	 * all values needed for numeric aggregates - wastes a bit of space, but
	 * saves casting and makes code simpler.
	 */

	protected class AggrResult {
		int count;
		double doubleVal;
	}


	protected FeedbackPunctuation translateForAggregate(FeedbackPunctuation fp)
	{
		FeedbackType _type;
		 ArrayList<String> _variables = new ArrayList<String>();
		 ArrayList<Comparator> _comparators = new ArrayList<Comparator>();
		 ArrayList<String> _values = new ArrayList<String>();
					
		Iterator<String> iter = fp.Variables().iterator();
		
		int posPointer = 0;
		
		String fattrNames[] = fAttr.split(" ");
		
		while(iter.hasNext())
		{
			String var = iter.next();			
			for(String s:fattrNames)
			{
				if(var.equals(s))
				{
					_variables.add(var);
					_comparators.add(fp.Comparators().get(posPointer));
					_values.add(fp.getValue(posPointer));
				}
			}	
			
			posPointer++;			
		}
		
		_variables.trimToSize();
		_comparators.trimToSize();
		_values.trimToSize();

		return new FeedbackPunctuation(fp.Type(),_variables,_comparators,_values);
	}

}
