/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.physical;


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

import org.w3c.dom.*;

class NoContentNoAttrsMerge extends MergeObject {

    /**
     * Constructor - does nothing
     */
    NoContentNoAttrsMerge(MergeTree mergeTree) {
	this.mergeTree = mergeTree;
	return;
    }

    /** accumulates the specified fragment element into the accumulator
     *
     * @param accumElt The element to be accumulated into (accumulator)
     * @param fragElt The element to accumulate into accumElt (accumulatee)
     *
     * @return Returns nothing.
     */
     void accumulate(Element accumElt, Element fragElt) {
	/* Return without doing anything.  Since this is a NoContentNoAttrs,
	 * means there is no content to deal with, 
	 * but we should create an empty element, if necessary
	 */
	 return;
    }

    Element accumulateEmpty(Element fragElt, String accumTagName) {
	/* Return without doing anything.  Since this is a NoContentNoAttrs,
	 * means there is no content to deal with, 
	 * but we should create an empty element, if necessary
	 */
	return createNewAccumElt(accumTagName);
    }

    /**
     * Just need a result element with the appropriate tag name,
     * since this is NoContentNoAttrs - means there are no attributes
     * or content to deal with
     *
     * @param rElt "right" element to be merged
     * @param lElt "left" element to be merged
     *
     * @return Returns the result element
     */
    Element merge(Element rElt, Element lElt, Document resDoc,
		  String tagName) {
	Element resElt = resDoc.createElement(tagName);
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
