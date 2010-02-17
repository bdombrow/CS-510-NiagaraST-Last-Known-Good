package niagara.physical;

import niagara.utils.Arithmetics;
import niagara.utils.BaseAttr;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;

import org.w3c.dom.Node;

/**
 * This is the <code>PhysicalAverageOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of average (a form
 * of grouping)
 * 
 * @version 1.0
 * 
 */

public class PhysicalAverage extends PhysicalAggregate {

	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */
	public void updateAggrResult(PhysicalAggregate.AggrResult result,
			BaseAttr ungroupedResult) {
		// Object ungroupedResult) {
		result.count++;
		result.value = ((Arithmetics) result.value).plus(ungroupedResult);
		// result.doubleVal += ((Double) ungroupedResult).doubleValue(); // sum
	}

	// ///////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific //
	// average operator (specializing the group operator) //
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
		// double sum = 0;
		BaseAttr sum = null;

		if (partialResult != null) {
			numValues += partialResult.count;
			sum = partialResult.value;
			// sum += partialResult.doubleVal;
		}
		if (finalResult != null) {
			numValues += finalResult.count;
			// sum += finalResult.doubleVal;
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

		// Create an average result element and return it
		return sum;
		/*
		 * Element resultElement = doc.createElement("Average"); Text
		 * childElement = doc.createTextNode(Double.toString(sum/numValues));
		 * resultElement.appendChild(childElement); return resultElement;
		 */
	}

	protected PhysicalAggregate getInstance() {
		return new PhysicalAverage();
	}
}
