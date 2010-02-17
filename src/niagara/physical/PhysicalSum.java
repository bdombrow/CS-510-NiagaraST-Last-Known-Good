package niagara.physical;

import niagara.utils.Arithmetics;
import niagara.utils.BaseAttr;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

/**
 * This is the <code>PhysicalSumOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of Sum (a form of
 * grouping)
 * 
 * @version 1.0
 * 
 */

public class PhysicalSum extends PhysicalAggregate {

	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */
	public void updateAggrResult(PhysicalAggregate.AggrResult result,
			BaseAttr ungroupedResult) {
		result.count++;
		// result.doubleVal holds the sum...
		// result.doubleVal += ((Double) ungroupedResult).doubleValue();
		result.value = ((Arithmetics) result.value).plus(ungroupedResult);
	}

	// ///////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific //
	// sum operator (specializing the group operator) //
	// ///////////////////////////////////////////////////////////////////////

	/**
	 * This function constructs a ungrouped result from a tuple
	 * 
	 * @param tupleElement
	 *            The tuple to construct the ungrouped result from
	 * 
	 * @return The constructed object; If no object is constructed, returns null
	 */

	protected final BaseAttr constructUngroupedResult(Tuple tupleElement)
			throws ShutdownException {
		// return getDoubleValue(tupleElement);
		return getValue(tupleElement);
	}

	/**
	 * This function returns an empty result in case there are no groups
	 * 
	 * @return The result when there are no groups. Returns null if no result is
	 *         to be constructed
	 */
	protected final Node constructEmptyResult() {
		return null;
	}

	/**
	 * This function constructs a result from the grouped partial and final
	 * results of a group. Both partial result and final result cannot be null
	 * 
	 * @param partialResult
	 *            The partial results of the group (this can be null)
	 * @param finalResult
	 *            The final results of the group (this can be null)
	 * 
	 * @return A results merging partial and final results; If no such result,
	 *         returns null
	 */

	protected final BaseAttr constructAggrResult(
			PhysicalAggregate.AggrResult partialResult,
			PhysicalAggregate.AggrResult finalResult) {

		// Create number of values and sum of values variables
		int numValues = 0;
		BaseAttr sum = null;
		// double sum = 0;

		if (partialResult != null) {
			numValues += partialResult.count;
			sum = partialResult.value;
			// sum += partialResult.doubleVal;
		}
		if (finalResult != null) {
			numValues += finalResult.count;
			if (sum != null)
				sum = ((Arithmetics) sum).plus(finalResult.value);
			else
				sum = partialResult.value;
		}

		// If the number of values is 0, average does not make sense
		if (numValues == 0) {
			assert false : "KT don't think returning null is ok";
			// return null;
		}

		// Create a sum result element
		return sum;
		/*
		 * Element resultElement = doc.createElement("Sum"); Text childElement =
		 * doc.createTextNode(Double.toString(sum));
		 * resultElement.appendChild(childElement); return resultElement;
		 */
	}

	protected PhysicalAggregate getInstance() {
		return new PhysicalSum();
	}
}
