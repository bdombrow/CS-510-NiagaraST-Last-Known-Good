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

import org.w3c.dom.*;
import java.io.*;

import niagara.utils.*;

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
    DeepReplaceMerge(int domSide) {
	if(domSide == MergeTree.DS_RIGHT) {
	    keepLeft = false;
	} else if (domSide == MergeTree.DS_LEFT) {
	    keepLeft = true;
	} else {
	    throw new PEException("Invalid dominant side");
	}
    }

    /** accumulates the specified fragment element into the accumulator
     *
     * @param accumElt The element to be accumulated into (accumulator)
     * @param fragElt The element to accumulate into accumElt (accumulatee)
     *
     * @return Returns nothing.
     * @exception OpExecException Thrown if exact match criteria isn't met
     */
    void accumulate(Element accumElt, Element fragElt) 
	throws OpExecException {

	/* convention - accumulator is always left */

	/* Don't need to do anything special for the case when
	 * there is an empty accumlElt (as may occur when we
	 * start with a null accumulator)
	 */

	/* if keepLeft => accumulator is dominant =>
	 * there is nothing to do
	 */
	if(!keepLeft) {
	    replaceElement(accumElt, fragElt);
	    // old - del when new working: accumElt.replaceYourself(fragElt); 
	} 
	return;
    }

    /** 
     * replaces one element with another
     *
     * @param old element to be replaced
     * @param new element to be put in place of "old"
     *
     */
    void replaceElement(Element oldElt, Element newElt)
	throws DOMException {
	oldElt.getParentNode().replaceChild(newElt, oldElt);
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
    Element merge(Element rElt, Element lElt, Document resDoc,
		  String tagName) 
	throws OpExecException {
	Element resElt = null;
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

    public void dump(PrintStream os) {
	if(keepLeft == true) {
	    os.println("Deep Replace Merge: keep left");
	} else {
	    os.println("Deep Replace Merge: keep right");
	}
    }

    public String toString() {
	if(keepLeft == true) {
	    return "Deep Replace Merge: keep left";
	} else {
	    return "Deep Replace Merge: keep right";
	}
    }

    public String getName() {
	return "Deep Replace";
    }
}
