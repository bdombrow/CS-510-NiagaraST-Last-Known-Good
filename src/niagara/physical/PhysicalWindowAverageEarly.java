package niagara.physical;

import java.util.ArrayList;

import niagara.logical.WindowAggregate;
import niagara.logical.WindowAverageEarly;
import niagara.optimizer.colombia.LogicalOp;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.FeedbackPunctuation;
import niagara.utils.FeedbackType;
import niagara.utils.Log;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
import niagara.utils.Tuple;

/**
 * This is the <code>PhysicalCountOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of Count (a form
 * of grouping)
 * 
 * @version 1.0
 * 
 */
@SuppressWarnings("unchecked")
public class PhysicalWindowAverageEarly extends PhysicalWindowAggregate {

	int totalCost = 0;
	
	// Early window closure threshold - a userspecified count that is sufficient to get a good estimate for the window-average
	String thresholdCount = "";
	long lastFPWid = -1;
	
	ArrayList values = new ArrayList();
	
	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */

	protected void localInitFrom(LogicalOp logicalOperator) {
		aggrAttr = ((WindowAggregate) logicalOperator).getAggrAttr();
		widName = ((WindowAggregate) logicalOperator).getWid();
		
		logging = ((WindowAggregate) logicalOperator).getLogging();
		if (logging) {
			log = new Log(this.getName());
		}
		
		exploit = ((WindowAggregate) logicalOperator).getExploit();
		
		thresholdCount = ((WindowAverageEarly) logicalOperator).getThresholdCount();
	}

	public void updateAggrResult(PhysicalWindowAggregate.AggrResult result,
			Object ungroupedResult) {
		// Increment the number of values
		// KT - is this correct??
		// code from old mrege results:
		// finalResult.updateStatistics(((Integer) ungroupedResult).intValue());
		if (ae.size() == 1) {
			result.count++;
			// result.doubleVal +=
			// ((Double)((Vector)ungroupedResult).get(0)).doubleValue(); // sum
			result.doubleVal += ((Double) ungroupedResult).doubleValue(); // sum
		} else {
			// the first item is sum; the second is count;
			result.doubleVal += ((Double) ((ArrayList) ungroupedResult).get(0))
					.doubleValue();
			result.count += ((Integer) ((ArrayList) ungroupedResult).get(1))
					.doubleValue();
		}
		
		if(result.count > Long.parseLong(thresholdCount) && curWidFrom > lastFPWid)
		{
//			 construct the FP element
			//String[] varsS = fattrs.split(" ");
			//String[] compsS = comparators.split(" ");
			//String[] valsS = values.split(" ");
			
			ArrayList<String> vars = new ArrayList<String>();
			ArrayList<FeedbackPunctuation.Comparator> comps = new ArrayList<FeedbackPunctuation.Comparator>();
			ArrayList<String> vals = new ArrayList<String>();

			vars.add("wid_from_" + widName);
			vals.add(String.valueOf(curWidFrom));
			comps.add(FeedbackPunctuation.Comparator.LE);

			
			vars.trimToSize();
			vals.trimToSize();
			comps.trimToSize();
			
			// Send elements
			FeedbackPunctuation fp = new FeedbackPunctuation(FeedbackType.ASSUMED, vars, comps, vals);
			try{
				sendFeedbackPunctuation(fp, 0);
			}catch (ShutdownException se){
				
			}catch (InterruptedException se){
				
			}
			
			if(logging){
		//		log.Update(fp.toString(), String.valueOf(tupleOut));
			}
			
			System.out.println(this.getName() + fp.toString());
			
			lastFPWid = curWidFrom;
		}
			
	}

	void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
			throws java.lang.InterruptedException, ShutdownException {
		// downstream control message is GET_PARTIAL
		// We should not get SYNCH_PARTIAL, END_PARTIAL, EOS or NULLFLAG
		// REQ_BUF_FLUSH is handled inside SinkTupleStream
		// here (SHUTDOWN is handled with exceptions)

		if (ctrl == null) {
			return;
		} else {
			// System.err.println(this.getName() + " is not null ");
		}

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

		switch (ctrlFlag) {
		case GET_PARTIAL:
			processGetPartialFromSink(streamId);
			break;
		case MESSAGE:
			FeedbackPunctuation fp = (FeedbackPunctuation) ctrl.get(2);
			//FeedbackPunctuation fpSend = new FeedbackPunctuation(fp.Type(),fp.Variables(),fp.Comparators(),fp.Values());
			
			if(logging){
				log.Update(fp.toString(), String.valueOf(tupleOut));
			}
			
			FeedbackPunctuation fpSend = new FeedbackPunctuation(fp.Type(),fp.Variables(),fp.Comparators(),fp.Values());
			
			fpSend = translateForAggregate(fpSend);
			
			// get attribute positions from tuple to check against guards
			names = new String[fpSend.Variables().size()];
			names = fpSend.Variables().toArray(names);

			// get positions
			positions = new int[fpSend.Variables().size()];
			for (int i = 0; i < names.length; i++) {
				positions[i] = outputTupleSchema.getPosition(names[i]);
			}
			
			if(exploit)
				outputGuard.add(fp);
			//System.out.println(this.getName() + "exploits" + fp.toString());
			
			if (propagate) {
				sendFeedbackPunctuation(fpSend, streamId);
				System.out.println(this.getName() + fp.toString());
			}
			
			break;
		default:
			assert false : "KT unexpected control message from sink "
					+ ctrlFlag.flagName();
		}
	}


	// //////////////////////////////////////////////////////////////////
	// These are the private variables of the class //
	// //////////////////////////////////////////////////////////////////

	// This is the aggregating attribute for the Count operator
	// Attribute countingAttribute;

	// ///////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific //
	// Count operator (specializing the group operator) //
	// ///////////////////////////////////////////////////////////////////////

	/**
	 * This function constructs a ungrouped result from a tuple
	 * 
	 * @param tupleElement
	 *            The tuple to construct the ungrouped result from
	 * 
	 * @return The constructed object; If no object is constructed, returns null
	 */

	protected final Object constructUngroupedResult(Tuple tupleElement) {

		// First get the atomic values
		atomicValues.clear();
		values.clear();
		if (ae.size() == 1) {
			ae.get(0).getAtomicValues(tupleElement, atomicValues);
			assert atomicValues.size() == 1 : "Must have exactly one atomic value";
			return new Double(((BaseAttr) atomicValues.get(0)).toASCII());
		} else {
			assert ae.size() == 2 : "Must have at most 2 aggregate attributes for average";
			ae.get(0).getAtomicValues(tupleElement, atomicValues);
			values.add(new Double(((BaseAttr) atomicValues.get(0)).toASCII()));

			atomicValues.clear();
			ae.get(1).getAtomicValues(tupleElement, atomicValues);
			values.add(new Integer(((BaseAttr) atomicValues.get(0)).toASCII()));
			return values;

		}
	}

	/**
	 * This function returns an empty result in case there are no groups
	 * 
	 * @return The result when there are no groups. Returns null if no result is
	 *         to be constructed
	 */

	protected final BaseAttr constructEmptyResult() {
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
			PhysicalWindowAggregate.AggrResult partialResult,
			PhysicalWindowAggregate.AggrResult finalResult) {
		// Create number of values and sum of values variables
		int numValues = 0;
		double sum = 0;
		double timestamp = 0;

		if (partialResult != null) {
			numValues += partialResult.count;
			sum += partialResult.doubleVal;
		}
		if (finalResult != null) {
			numValues += finalResult.count;
			sum += finalResult.doubleVal;
		}

		// If the number of values is 0, average does not make sense
		if (numValues == 0) {
			assert false : "KT don't think returning null is ok";
			// return null;
		}

		// Create an average result element and return it
		totalCost += timestamp;
		StringAttr resultElement = new StringAttr(sum / numValues);
		return resultElement;

	}

	protected PhysicalWindowAggregate getInstance() {
		return new PhysicalWindowAverageEarly();
	}

	protected PhysicalWindowGroup localCopy() {
		PhysicalWindowAverageEarly op = (PhysicalWindowAverageEarly)super.localCopy();
		op.thresholdCount = thresholdCount;
		
		return op;
	}

	public int hashCode() {
		return super.hashCode() ^ thresholdCount.hashCode();
	}
}
