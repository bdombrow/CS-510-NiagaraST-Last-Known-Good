/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;


/**
 * The <code>DeepReplace</code> class which merges two
 * elements by replacing one element with the other
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import niagara.utils.nitree.*;

class DeepReplaceMerge extends MergeObject {

    /* this variable must be set during creation - it
     * tells which side should be taken as the result
     * value
     */
    private boolean keepLeft;

    /**
     * Constructor - initializes the one and only member variable
     *
     * @param keepLeft boolean variable indicating which side should
     *    be kept (replaced)
     *    
     */
    DeepReplaceMerge(boolean _keepLeft) {
	keepLeft = _keepLeft;
    }

    /** accumulates the specified fragment element into the accumulator
     *
     * @param accumElt The element to be accumulated into (accumulator)
     * @param fragElt The element to accumulate into accumElt (accumulatee)
     *
     * @return Returns nothing.
     * @exception OpExecException Thrown if exact match criteria isn't met
     */
    void accumulate(NIElement accumElt, NIElement fragElt) 
	throws OpExecException {

	/* convention - accumulator is always left */

	/* if keepLeft => accumulator is dominant =>
	 * there is nothing to do
	 */
	if(!keepLeft) {
	    accumElt.replaceYourself(fragElt);
	} 

	return;
    }

    /**
     * merges the two elements into the result element
     *
     * @param rElt "right" element to be merged
     * @param lElt "left" element to be merged
     *
     * @return Returns the result element
     */
    NIElement merge(NIElement rElt, NIElement lElt, NIDocument resDoc,
		    String tagName) 
	throws OpExecException {
	NIElement resElt = null;
	if(keepLeft) {
	    resElt = lElt;
	} else {
	    resElt = rElt;
	}

	/* do some checking */
	if(resElt.getOwnerDocument() != resDoc ||
	   resElt.getTagName() != tagName) {
	    throw new 
		OpExecException("Tag or doc doesn't match in deep replace merge");
	}

	return resElt;
    }

    /**
     * Indicates if merge is "deep" or not and implies whether recursion
     * should continue after this merge is completed.
     *
     * @return Returns true
     */
    boolean isDeepMerge() {
	return true;
    }
}
