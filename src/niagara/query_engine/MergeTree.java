/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

/**
 * Implementation of a <code> MergeTree </code> class which
 * merges two XML documents together. This file also contains the
 * implementation of <code> MergeTreeNode </code>.  The merge tree
 * is constructed of MergeTreeNodes.  MergeTree is a wrapper for the
 * tree constructed of MergeTreeNodes and also handles the treewalking.
 * MergeTreeNodes make up the tree structure and the Merge and Match
 * objects which are members of each MergeTreeNode, do the real merging
 * and matching work.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.util.*;
import org.w3c.dom.*;

import niagara.utils.nitree.*;


public class MergeTree {
    
    /*
     * DATA MEMBERS
     */
    private MergeTreeNode mergeTreeRoot;
    private Document      resultDoc;

    /*
     * METHODS
     */

    /**
     * Constructor - does nothing now.
     *
     */
     
    public MergeTree() { 
	mergeTreeRoot = null;
	resultDoc = null;
    }

    /**
     * Creates the MergeTree instance from a DOM document
     * consisting of the MergeTemplate. Traverses the merge
     * template and creates the appropriate MergeTreeNodes.
     *
     * @param mergeTemplate DOM document containing the XML merge template
     *
     * @return Returns nothing.
     */
    public void create(Document mergeTemplate) {
	/* creates root MergeTreeNode and recursively creates 
	 * MergeTreeNodes (one for each element in the merge template) 
	 * to creates the whole merge tree.
	 */
	mergeTreeRoot = new MergeTreeNode(mergeTemplate.getDocumentElement());
	/* ???  To Implement !!! */
	return;
    }

    /**
     * Resets the instance variables to null.
     */
    public void reset() {
	mergeTreeRoot = null;
	resultDoc = null;
	return;
    }

    /**
     * Merges the an XML Element into an XML Document as specified
     * by the merge tree associated with the class.
     * For now, this is not general enough to handle the join (merge two
     * fragments and create a new result) - maybe it will be some day
     *
     * @param accumDoc     The document into which the fragment
     *                       will be merged
     * @param fragment     The fragment to be merged into the document
     *                       later this may be of type XMLFragment or
     *                       something - for now it is just a normal tree
     */

    void accumulate(NIDocument _accumDoc, NIElement _fragment) 
	throws OpExecException {
	/*
         * make it left and right frag and make treewalking code
	 * general enough to handle the join code?
	 */
	NIElement accumFragRoot = _accumDoc.getDocumentElement(); 
	NIElement mergeFragRoot = _fragment; 

	do_accumulate(accumFragRoot, mergeFragRoot, mergeTreeRoot);

	resultDoc = null;
    }

    /** Recursive function to walk the merge tree, accumulator (result),
     *  and the fragment and do the merge.
     * The actual merging of elements and matching
     * is done by merge and match objects.  The objective of this function
     * is to handle the tree walking and locating which elements to call
     * merge and match on. 
     *
     * @param accumElt The current element of the accumulate document (result)
     * @param fragElt The current element of the fragment
     * @param mergeTreeNode The current node of the merge tree
     *
     */

    private void do_accumulate(NIElement accumElt, NIElement fragElt, 
			       MergeTreeNode mergeTreeNode) 
	throws OpExecException {

	/* in all cases, I get in two elements that should
	 * immediately be merged - root, scalar, and set
	 * In set case, match
	 * has been done before this function was called
	 * what if either one is null???? can this happen??
	 */
	mergeTreeNode.accumulate(accumElt, fragElt);

	if(!mergeTreeNode.isDeepMerge()) {
	    /* now call recursively - we scan the subelements of fragElt 
	     * and look for subelements of the mergeElt with the same tag 
	     * name - if found, then get the subelement from accumElt with
	     * matching tag, if available, and make the recursive call.  
	     * If no existing subelement in accumElt, then maybe just insert 
	     * fragElt (without it's children??) into accumElt
	     * for now, we guess this is the most efficient (see 5-24-00 notes)
	     */
	    NIElement nextFragElt = fragElt.getFirstChildElement();
	    NIElement nextAccumElt;
	    while(nextFragElt != null) {
		String fragName = fragElt.getTagName();
		
		/* could avoid this wierd lookup by scanning merge tree 
		 * first - but that could be inefficient??
		 */
		MergeTreeNode nextMergeTreeNode = 
		    mergeTreeNode.getChildWithFragTagName(fragName);
		String accumName = nextMergeTreeNode.getAccumTagName();
		
		/* What if multiple children match??
		 * In case where nextMergeTreeNode.resultType()==scalar, then 
		 * there will be at most one accumElt child with name accumName
		 * In case where nextMergeTreeNode.resultType() == set, there
		 * may be many children of accumElt with accumName - need
		 * to call a match to figure out which one matches - we
		 * get the match out of some hash table - maintained where??
		 * (NOTE we have this exact same problem whether we scan
		 * mergeTree or fragTree first)
		 */
		
		if(nextMergeTreeNode.resultType() == MergeTreeNode.SCALAR) {
		    nextAccumElt = accumElt.getChildByName(accumName);
		}   else { 
		    /* result type must be set 
		     * NOTE - can't be multiple matches since this
		     * is accumulate - could be if it were merge
		     * Multiple frags can match to one accum and there
		     * might be multiple accum Children with tag name
		     * same as nextFragTagName, but
		     * that doesn't cause a set result from findMatch
		     */
		    nextAccumElt = nextMergeTreeNode.findUniqueMatch(accumElt, 
							      nextFragElt);
		} 
		
		/* the recursive call */
		do_accumulate(nextAccumElt, nextFragElt, nextMergeTreeNode);
		
		/* now process the next sibling element */
		nextFragElt = fragElt.getNextSiblingElement();
	    }
	}
	
	return;
    }

    /**
     * Merges two XML Documents together to produce a new result
     * XML Document as specified by the merge tree associated with
     * this class.
     *
     * @param lDoc "left" document to be merged
     * @param rDoc "right" document to be merged
     *
     * @return Returns the new result document 
     */

    NIDocument merge(NIDocument lDoc, NIDocument rDoc) {
	return lDoc;  /* make the compiler happy for now ??? */
    }
}

/** <code> MergeTreeNode </code> class.  The merge tree is made up of 
 * <code> MergeTreeNode </code> s.  A
 * merge tree will be created from parsing the XML representation of
 * the merge template. Each MergeTreeNode contains merge and match
 * objects which do the real merging and matching work.
 */
class MergeTreeNode {

    /*
     * DATA MEMBERS
     */
    private MergeObject merger;
    private MatchTemplate matcher;
    private ArrayList children;

    private String lInputTagName; 
    private String rInputTagName;
    private String resultTagName;
    private String fragTagName;
    private String accumTagName;

    private int resultType; /* is either "scalar" or "set" */
    static final int SCALAR = 1;
    static final int SET = 2;
    
    /* HashTable for doing match kept here?? */

    /*
     * METHODS
     */

    /**
     * Constructor - creates the a MergeTreeNode corresponding to 
     * mergeTemplateElement and recursively calls itself to create 
     * MergeTreeNodes for all of the children of mergeTemplateElement.
     */
    MergeTreeNode(Element mergeTemplateElement) {

	/* set lInputTagName, rInputTagName, resultTagName, resultType */

	/* left is accumulator and right is fragment - nothing special
	   about this - just a convention */
	accumTagName = lInputTagName;
	fragTagName = rInputTagName;
	
	/* create merge object */

	/* create match object */

	/* create children by recursively calling the constructor on 
	 * appropriate child elements of mergeTemplateElement 
	*/
	
    }


    /**
     * Returns the tag name of the accumulator element in the
     * merge represented by this MergeTreeNode.
     *
     */
    String getAccumTagName() {
	return accumTagName;
    }


    /**
     * Returns the tag name of the fragment element in the merge
     * represented by this MergeTreeNode.
     */
    String getFragTagName() {
	return fragTagName;
    }

    /**
     * Returns the result type of this merge - scalar or set
     */
    int resultType() {
	return resultType;
    }

    /**
     * Retrieves a child element whose value of fragment tag name is fragName.
     * Due to renaming, the fragment (accumulatee) tag name and accum 
     *(accumulator) tag name may be different 
     *
     * @param fragName String to be matched to the child fragment tag name
     *
     * @return The child of this MergeTreeNode with fragment tag name is
     * fragName.
     */
    MergeTreeNode getChildWithFragTagName(String fragName) {
	ListIterator iter = children.listIterator();
	
	MergeTreeNode child = null;
	if(iter.hasNext()) {
	    child = (MergeTreeNode) iter.next();
	}

	while(child != null && !child.getFragTagName().equals(fragName)) {
	    if(iter.hasNext()) {
		child = (MergeTreeNode) iter.next();
	    } else {
		child = null;
	    }
	}
	return child;
    }

    /**
     * Indicates if the merge is deep or not and implies whether the
     * recursion should continue after this merge has been completed
     *
     * @return Returns true/false to indicate if merge is deep or not 
     *
     */
    
    boolean isDeepMerge() {
	return merger.isDeepMerge();
    }

    /**
     * Merges two NIElements together using the merger member object.
     *
     * @param lElt "left" element to be merged
     * @param rElt "right" element to be merged
     * @param doc Document with which result is to be associated
     *
     * @return returns new result element 
     */
    NIElement merge(NIElement lElt, NIElement rElt, NIDocument doc) 
	throws OpExecException {
	return merger.merge(lElt, rElt, doc, resultTagName);
    }

    /**
     * Function to accumulate one NIElement into another. Will use
     * the merger member object to do the accumulate.
     *
     * @param accumElt NIElement to be accumulated into (accumulator)
     * @param fragElt NIElement to be accumulated into accumElt (accumulatee)
     *
     * @return Returns true or false to indicate whether recursion 
     *         should continue or not 
     */
    void accumulate(NIElement accumElt, NIElement fragElt) 
	throws OpExecException {
	merger.accumulate(accumElt, fragElt);
    }

    /**
     * Finds the only child of accumElt which "matches" nextFragElt - uses
     * the matcher member to do this. If there are multiple matches
     * in accumElt, this is an error.
     *
     * @param accumElt Accumulator element - look for matches among this 
     * element's children
     * @param nextFragElt Fragment element - matching this fragment with
     * accumElt's kids
     *
     * @return Returns matching element which is a child of accumElt. 
     */
    NIElement findUniqueMatch(NIElement accumElt, NIElement nextFragElt) {
	return matcher.findUniqueMatch(accumElt, nextFragElt);
    }
}





