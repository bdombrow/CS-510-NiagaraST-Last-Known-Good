package niagara.physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;

import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.IntegerAttr;
import niagara.utils.Punctuation;
import niagara.utils.ShutdownException;
import niagara.utils.StringAttr;
import niagara.utils.Tuple;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * This is the <code>PhysicalCountOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of Count (a form
 * of grouping)
 * 
 * @version 1.0
 * 
 */
@SuppressWarnings("unchecked")
public class PhysicalWindowCount extends PhysicalWindowAggregate {

	// We need a sample tuple in order to create a feedback punctuation
	Tuple tupleDataSample = null;

	/**
	 * This function updates the statistics with a value
	 * 
	 * @param newValue
	 *            The value by which the statistics are to be updated
	 */
	public void updateAggrResult(PhysicalWindowAggregate.AggrResult result,
			Object ungroupedResult) {
		// Increment the number of values
		// KT - is this correct??
		// code from old mrege results:
		// finalResult.updateStatistics(((Integer) ungroupedResult).intValue());

		// int size = ((Vector)ungroupedResult).size();
		// assert ((Integer)((Vector)ungroupedResult).get(0)).intValue() == 1 :
		assert ((Integer) ungroupedResult).intValue() == 1 : "KT BAD BAD BAD";

		result.count++;
	}

	void processCtrlMsgFromSink(ArrayList ctrl, int streamId)
			throws java.lang.InterruptedException, ShutdownException {
		if (ctrl == null) {
			return;
		} else {
			// We can continue assuming ctrl has something in it
		}

		ControlFlag ctrlFlag = (ControlFlag) ctrl.get(0);

		switch (ctrlFlag) {
		case GET_PARTIAL:
			processGetPartialFromSink(streamId);
			break;
		case MESSAGE:
			System.err.println(this.getName() + "***Got message: "
					+ ctrl.get(1) + " with propagate =  " + propagate);
			String[] feedback = ctrl.get(1).toString().split("#");
			fAttr = feedback[0];
			guardOutput = feedback[1];
			if (propagate) {
				sendCtrlMsgUpStream(ctrlFlag, ctrl.get(1).toString(), 0);
			}
			Punctuation feedbackPunctuation = createFeedbackPunctuation(fAttr,
					guardOutput);
			processFeedbackPunctuation(feedbackPunctuation);
			System.out.println("Purged internal state for: " + fAttr
					+ " with guard: " + guardOutput);
			break;
		default:
			assert false : "KT unexpected control message from sink "
					+ ctrlFlag.flagName();
			break;
		}
	}

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
		if (tupleDataSample == null) {
			tupleDataSample = (Tuple) tupleElement.clone();
		}

		// First get the atomic values
		atomicValues.clear();
		ae.get(0).getAtomicValues(tupleElement, atomicValues);

		assert atomicValues.size() == 1 : "Must have exactly one atomic value";

		return new Integer(1);
	}

	/**
	 * This function returns an empty result in case there are no groups
	 * 
	 * @return The result when there are no groups. Returns null if no result is
	 *         to be constructed
	 */

	protected final BaseAttr constructEmptyResult() {
		// Create an Count result element
		IntegerAttr resultElement = new IntegerAttr(0);

		// Return the result element
		return resultElement;
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
		int numValues = 0;
		// double timestamp = 0;

		if (partialResult != null)
			numValues += partialResult.count;

		if (finalResult != null)
			numValues += finalResult.count;

		IntegerAttr resultElement = new IntegerAttr(numValues);
		return resultElement;
	}

	protected PhysicalWindowAggregate getInstance() {
		return new PhysicalWindowCount();
	}

	/**
	 * This function generates a punctuation based on the timer value using the
	 * template generated by setupDataTemplate
	 */
	private Punctuation createFeedbackPunctuation(String attrName, String value) {

		// Element eChild;
		// Text tPattern;
		short nodeType;

		// Create a new punctuation element
		Punctuation spe = new Punctuation(false);
		for (int iAttr = 0; iAttr < tupleDataSample.size(); iAttr++) {
			Object ndSample = tupleDataSample.getAttribute(iAttr);

			// compare attrName to sample tuple attribute name
			// if match, add value instead of "*" to that attribute

			// TODO: Since tupleDataSample is a sample of input tuples I think
			// we
			// should be getting variables from the inputTupleSchema... I assume
			// that inputTupleSchmas[0] is the stream that we want

			// String iAttrName =
			// (outputTupleSchema.getVariable(iAttr)).getName();
			// BaseAttr.Type iAttrType =
			// outputTupleSchema.getVariable(iAttr).getDataType();
			String iAttrName = (inputTupleSchemas[0].getVariable(iAttr))
					.getName();
			BaseAttr.Type iAttrType = inputTupleSchemas[0].getVariable(iAttr)
					.getDataType();

			if (ndSample instanceof BaseAttr) {
				if (!iAttrName.equals(attrName)) {
					// spe.appendAttribute(((BaseAttr)
					// ndSample).createWildStar(iAttrType));
					spe.appendAttribute(BaseAttr.createWildStar(iAttrType));
				} else {
					((BaseAttr) ndSample).loadFromObject(value);
					spe.appendAttribute((BaseAttr) ndSample);
				}
			} else if (ndSample instanceof Node) {
				nodeType = ((Node) ndSample).getNodeType();
				if (nodeType == Node.ATTRIBUTE_NODE) {
					Attr attr = doc.createAttribute(((Node) ndSample)
							.getNodeName());
					if (!iAttrName.equals(attrName)) {
						attr.appendChild(doc.createTextNode("*"));
					} else {
						attr.appendChild(doc.createTextNode(value));
					}
					spe.appendAttribute(attr);
				} else {
					String stName = ((Node) ndSample).getNodeName();
					if (stName.compareTo("#document") == 0)
						stName = new String("document");
					Element ePunct = doc.createElement(stName);
					if (!iAttrName.equals(attrName)) {
						ePunct.appendChild(doc.createTextNode("*"));
					} else {
						ePunct.appendChild(doc.createTextNode(value));
					}
					spe.appendAttribute(ePunct);
				}
			}
		}
		spe.appendAttribute(new StringAttr(value));
		return spe;
	}

	protected void processFeedbackPunctuation(Punctuation inputTuple)
			throws ShutdownException, InterruptedException {

		// System.err.println(this.id + " process punct");

		// HashEntry groupResult = null;
		if (numGroupingAttributes == 0)
			assert false : "not supported yet - yell at Kristin";

		String stPunctKey;
		try {
			stPunctKey = hasher.hashKey(inputTuple);
		} catch (java.lang.ArrayIndexOutOfBoundsException ex) {
			// Not a punctuation for the group attribute. Ignore it.
			return;
		}

		String[] punctValues = new String[numGroupingAttributes];
		hasher.getValuesFromKey(stPunctKey, punctValues);

		// Does the punctuation punctuates on every grouping attribiute?
		boolean idealPunct = true;
		for (int i = 0; i < numGroupingAttributes; i++)
			if (punctValues[i].trim().compareTo("*") == 0) {
				idealPunct = false;
				break;
			}

		// Yes. The punctuation punctuates on every grouping attributes
		if (idealPunct) {
			// groupResult = (HashEntry) hashtable.get(stPunctKey);

			/*
			 * else { int pos =
			 * inputTupleSchemas[0].getPosition("wid_from_"+inputName); String
			 * tmpPos =
			 * inputTuple.getAttribute(pos).getFirstChild().getNodeValue(); }
			 */// for debugging
			hashtable.remove(stPunctKey);
		}
		// No. Need to walk through the hashtable
		else {
			// Iterator keys = hashtable.keySet().iterator();
			Set keys = hashtable.keySet();

			LinkedList removes = new LinkedList();

			String groupKey;
			// Object next;
			String[] groupKeyValues = new String[numGroupingAttributes];
			boolean match = false;
			// while (keys.hasNext()) {
			for (Object key : keys) {
				// next = keys.next();
				// groupKey = (String)next;
				groupKey = (String) key;
				hasher.getValuesFromKey(groupKey, groupKeyValues);
				match = true;
				for (int i = 0; i < numGroupingAttributes; i++) {
					// if groupKeyValues match punctVals, output the result
					if (groupKeyValues[i].compareTo(punctValues[i]) != 0
							&& punctValues[i].trim().compareTo("*") != 0) {
						match = false;
						break;
					}
				}
				if (match) {
					// keys.remove();
					// removes.add(next);
					removes.add(key);
				}
			}

			for (Object key : removes) {
				hashtable.remove(key);
			}

		}
	}
}
