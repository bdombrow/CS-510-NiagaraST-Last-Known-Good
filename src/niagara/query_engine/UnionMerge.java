/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;


/**
 * The <code>UnionMerge</code> class which merges two
 * elements by effectively concatenating them - no comparisons
 * are done - both elements are included in result
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import org.w3c.dom.*;
import java.io.*;

import niagara.utils.*;

class UnionMerge extends MergeObject {

    /**
     * Constructor 
     *
     */
    UnionMerge(MergeTree mergeTree) {
	this.mergeTree = mergeTree;
    }

    /** accumulates the specified fragment element into the accumulator
     *
     * @param accumElt The element to be accumulated into (accumulator)
     * @param fragElt The element to accumulate into accumElt (accumulatee)
     *
     * @return Returns nothing.
     */
    void accumulate(Element accumElt, Element fragElt) {

	/* Do we need to do anything special for the case when
	 * there is an empty accumlElt (as may occur when we
	 * start with a null accumulator)? Wait yes - need to replace it
	 */
	Node importedNode = mergeTree.accumDoc.importNode(fragElt, true);
	accumElt.getParentNode().appendChild(importedNode);
    }


    Element accumulateEmpty(Element fragElt, String accumTagName) {

	/* Do we need to do anything special for the case when
	 * there is an empty accumlElt (as may occur when we
	 * start with a null accumulator)? Wait yes - need to replace it
	 */
	return (Element)mergeTree.accumDoc.importNode(fragElt, true);
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
		  String tagName) {
	throw new PEException("KT: Shouldn't call merge on union - can't find where this is called");
    }

    /**
     * Indicates if merge is "deep" or not and implies whether recursion
     * should continue after this merge is completed.
     *
     * @return Returns false
     */
    boolean isDeepMerge() {
	return true;
    }

    public void dump(PrintStream os) {
	os.println("Union Merge");
    }

    public String toString() {
	return "Union Merge";
    }

    public String getName() {
	return "Union";
    }
}
