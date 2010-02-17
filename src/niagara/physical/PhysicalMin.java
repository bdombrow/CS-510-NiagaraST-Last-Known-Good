package niagara.physical;

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

public class PhysicalMin extends PhysicalAggregate {

	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */
	public void updateAggrResult(PhysicalAggregate.AggrResult result,
			BaseAttr ungroupedResult) {
		// Object ungroupedResult) {
		// increm num values and update the max
		/*
		 * double newValue = ((Double) ungroupedResult).doubleValue();
		 * result.count++; if (result.doubleVal == 0) result.doubleVal =
		 * newValue; if (newValue < result.doubleVal) // doubleVal holds max
		 * result.doubleVal = newValue;
		 */
		result.count++;
		if (result.value != null) {
			if (ungroupedResult.lt(result.value))
				result.value = ungroupedResult;
		} else
			result.value = ungroupedResult;

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

	// protected final Object constructUngroupedResult (Tuple
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

	// protected final Node constructAggrResult (
	protected final BaseAttr constructAggrResult(
			PhysicalAggregate.AggrResult partialResult,
			PhysicalAggregate.AggrResult finalResult) {

		// Create number of values and sum of values variables
		int numValues = 0;
		// long min = Long.MAX_VALUE;
		BaseAttr min = null;

		if (partialResult != null) {
			numValues += partialResult.count;
			min = partialResult.value;
			/*
			 * if (partialResult.doubleVal < min) min = (long)
			 * partialResult.doubleVal;
			 */
		}
		if (finalResult != null) {
			numValues += finalResult.count;
			if (min != null) {
				if (finalResult.value.lt(min))
					min = finalResult.value;
			} else
				min = finalResult.value;
			/*
			 * if (finalResult.doubleVal < min) min =
			 * (long)finalResult.doubleVal;
			 */
		}

		// If the number of values is 0, sum does not make sense
		if (numValues == 0) {
			assert false : "KT don't think returning null is ok";
		}

		return min;
		/*
		 * Element resultElement = doc.createElement("niagara:min"); Text
		 * childElement = doc.createTextNode(Long.toString(min));
		 * resultElement.appendChild(childElement); return resultElement;
		 */
	}

	protected PhysicalAggregate getInstance() {
		return new PhysicalMin();
	}
}
