package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */



/**
 * Interface definition for the <code> MergeObject </code>.  All merge
 * objects take in two XML elements and merge them together - either
 * in a merge or accumulate sense.  There will be one implementation
 * of MergeObject for each type of merge.
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import niagara.utils.nitree.*;

abstract class MergeObject {

    /* Initialization will be done differently for all merge objects
     * since they need such different information.
     */

    /** accumulates the specified fragment element into the accumulator
     *
     * @param accumElt The element to be accumulated into (accumulator)
     * @param fragElt The element to accumulate into accumElt (accumulatee)
     *
     * @return Returns nothing 
     */
    abstract void accumulate(NIElement accumElt, NIElement fragElt) 
	throws OpExecException;

    /** 
     * merges two fragments together
     *
     * @param lElt "left" element to be merged
     * @param rElt "right" element to be merged
     * @param resDoc The document with which result elt is to be associated
     * @param tagName The tag name for the new result element 
     *
     * @return Returns a pointer to the result element
     */
    abstract NIElement merge(NIElement lElt, NIElement rElt, 
			     NIDocument resDoc, String tagName)
	throws OpExecException;


    /**
     * indicates if this merge is deep or shallow and implies whether
     * recursion should continue after this merge is completed
     */
    abstract boolean isDeepMerge();
}



