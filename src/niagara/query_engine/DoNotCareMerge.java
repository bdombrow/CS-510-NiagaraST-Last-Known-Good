/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;


/**
 * The <code>DoNotCare</code> class does nothing when
 * merge is called on it.  It is a placeholder for a
 * null op
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.io.*;

import niagara.utils.nitree.*;

class DoNotCareMerge extends MergeObject {

    /**
     * Constructor - does nothing
     */
    DoNotCareMerge() {
	return;
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
	/* Return without doing anything.  Since this is a DoNotCare,
	 * means there are no attributes or content to deal with
	 */
	return;
    }

    /**
     * Just need a result element with the appropriate tag name,
     * since this is DoNotCare - means there are no attributes
     * or content to deal with
     *
     * @param rElt "right" element to be merged
     * @param lElt "left" element to be merged
     *
     * @return Returns the result element
     */
    NIElement merge(NIElement rElt, NIElement lElt, NIDocument resDoc,
		    String tagName) 
	throws OpExecException, NITreeException {
	NIElement resElt = resDoc.createNIElement(tagName);
	return resElt;
    }

    /**
     * Indicates if merge is "deep" or not and implies whether recursion
     * should continue after this merge is completed.
     *
     * @return Returns true
     */
    boolean isDeepMerge() {
	return false;
    }

    public void dump(PrintStream os) {
	os.println(this);
    }

    public String toString() {
	return getName();
    }

    public String getName() {
	return "Do Not Care Merge";
    }

}
