/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

/**
 * Implementation of a <code> MergeTree </code> class which
 * merges two XML documents together.  The merge tree
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
import com.ibm.xml.parsers.*;
import org.xml.sax.*;
import java.io.*;

import niagara.utils.*;
import niagara.utils.nitree.*;


public class MergeTree {
    
    /*
     * DATA MEMBERS
     */
    private MergeTreeNode mergeTreeRoot;
    private Document      resultDoc;
    private boolean       validForAccumulate;
    private int           treeMergeType;
    private int           treeDomSide;
    
    /* values for MergeType and DominantSide */
    public static final int MT_OUTER = 1;
    public static final int MT_LEFTOUTER = 2;
    public static final int MT_RIGHTOUTER = 3;
    public static final int MT_INNER = 4;
    
    public static final int DS_LEFT = 1;
    public static final int DS_RIGHT = 2;
    public static final int DS_EXACTMATCH = 3;

    public static final String DEF_ATTR_STRING = "ATTR_DEFAULT";
    
    /*
     * METHODS
     */
    
    /**
     * Constructor - does nothing now.
     *
     */
    
    public MergeTree() { 
	reset();
    }

   /**
     * Creates the MergeTree instance from a string.  This function
     * is responsible for parsing the string appropriately and
     * then calling the create function which takds a DOM document
     * as an argument.
     *
     * @param mergeTemplate String representing the XML merge template.
     *       Can be a string containing the merge template, a file name,
     *          or a URI
     * @param checkAccumConstraints true/false depending on whether you
     *        want create to verify that this tree can be used
     *
     * @return Returns nothing.
     */    
    public void create(String mergeTemplateStr, boolean checkAccumConstraints) 
	throws MTException {

	try {

	    RevalidatingDOMParser p;
	    Document mergeTemplate = null;
	    p = new RevalidatingDOMParser();
	    
	    /* Parse the mergeTemplate file or string */
	    if(mergeTemplateStr.startsWith("<?xml")) {
		p.parse(new InputSource(new ByteArrayInputStream(mergeTemplateStr.getBytes())));
	    } else {
		p.parse(new InputSource(mergeTemplateStr));
	    }
	    create(p.getDocument(), checkAccumConstraints);
	} catch (java.io.IOException e) {
	    throw new MTException("Merge String appears corrupted " + 
				  e.getMessage());
	} catch (org.xml.sax.SAXException e) {
	    throw new MTException("Error parsing merge template " +
				  e.getMessage());
	}
	return;
    }

    /**
     * Creates the MergeTree instance from a DOM document
     * consisting of the MergeTemplate. Traverses the merge
     * template and creates the appropriate MergeTreeNodes.
     *
     * @param mergeTemplate DOM document containing the XML merge template
     * @param checkAccumConstraints true/false depending on whether you
     *        want create to verify that this tree can be used
     *        for accumulate - some configurations aren't allowed
     *        by accumulate and if accumConstraints is true, the
     *        existence of these conditions will cause an error to be thrown
     *
     * @return Returns nothing.
     */
    private void create(Document mergeTemplate, boolean checkAccumConstraints) 
	throws MTException {
	/* creates root MergeTreeNode and recursively creates 
	 * MergeTreeNodes (one for each element in the merge template) 
	 * to creates the whole merge tree.
	 */
	
	Element docElement = mergeTemplate.getDocumentElement();	

	setMergeType(docElement);
	setDominantSide(docElement);
	
	/* Find the first element child */
	Element mergeElt = ElementAssistant.getFirstElementChild(docElement);
	
	if(mergeElt == null) {
	    throw new MTException("Document element of merge template doesn't have any element children");
	}

	mergeTreeRoot = new MergeTreeNode(mergeElt, treeMergeType,
					  treeDomSide, checkAccumConstraints);

	/* make sure that was the only child */
	mergeElt = ElementAssistant.getNextElementSibling(mergeElt);
	if(mergeElt != null) {
	    throw new MTException("Unexpected Child Element of document element of merge template");
	}
	
	/* now that creation succeeded, indicate if this tree is
	 * valid to be used as an accumulate tree or not 
	 */
	validForAccumulate = checkAccumConstraints;
	return;
    }

    private void setMergeType(Element docElement) 
	throws MTException {
	setDominantSide(docElement);

	/* know we won't get empty strings here because these
	 * attrs have default values 
	 */
	String mergeType = docElement.getAttribute("MergeType");
	if(mergeType.equals("outer")) {
	    treeMergeType = MT_OUTER;
	} else if (mergeType.equals("left-outer")) {
	    treeMergeType = MT_LEFTOUTER; 
	} else if (mergeType.equals("right-outer")) {
	    treeMergeType = MT_RIGHTOUTER;
	} else if (mergeType.equals("inner")) {
	    treeMergeType = MT_INNER;
	} else {
	    throw new MTException("Invalid merge type");
	}
	return;
    }

    private void setDominantSide(Element docElement) 
	throws MTException {
	String domSide =  docElement.getAttribute("DominantSide");
	
	if(domSide.equals("left")) {
	    treeDomSide = DS_LEFT;
	} else if (domSide.equals("right")) {
	    treeDomSide = DS_RIGHT;
	} else if (domSide.equals("exactMatch")) {
	    treeDomSide = DS_EXACTMATCH;
	} else {
	    throw new MTException("Invalid Dominant Side");
	}
	return;
    }

    /**
     * Resets the instance variables to null.
     */
    public void reset() {
	mergeTreeRoot = null;
	resultDoc = null;
	treeMergeType = MT_OUTER;
	treeDomSide = DS_RIGHT;
	validForAccumulate = false;
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

    void accumulate(NIDocument accumDoc, NIElement fragment) 
	throws OpExecException, NITreeException {
	/*
         * make it left and right frag and make treewalking code
	 * general enough to handle the join code?
	 */
	NIElement accumFragRoot = accumDoc.getDocumentElement(); 
	NIElement mergeFragRoot = fragment; 

	/* have to handle the case when we start with an empty
	 * accumulator - in this case accumFragRoot is null, so
	 * we make an empty document element for the accumulator
	 * and root add it to the tree
	 */
	if(accumFragRoot == null) {
	    accumFragRoot = accumDoc.
		createNIElement(mergeTreeRoot.getAccumTagName());
	    accumDoc.appendChild(accumFragRoot);
	} else {
	    /* this automatically updates the accumDoc, if necessary */
	    accumFragRoot = accumFragRoot.makeWriteable();
	}

	do_accumulate(accumFragRoot, mergeFragRoot, mergeTreeRoot,
		      accumDoc);

	//System.out.println("End of accumulate");
	//accumDoc.printWithFormat();

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
			       MergeTreeNode mergeTreeNode,
			       NIDocument accumDoc) 
	throws OpExecException, NITreeException {

	boolean isBottom = true;

	/* in all cases, I get in two elements that should
	 * immediately be merged - root, scalar, and set
	 * In set case, match
	 * has been done before this function was called
	 * what if either one is null???? can this happen??
	 */
	/* System.out.println("Before accumulating " + accumElt.getTagName() +
	 *	   " element");
	 * accumDoc.printWithFormat();
	 */
	mergeTreeNode.accumulate(accumElt, fragElt);
	
	if(!mergeTreeNode.isDeepMerge()) {
	    /* now call recursively - we scan the subelements of fragElt 
	     * and look for subelements of the mergeElt with the same tag 
	     * name - if found, then get the subelement from accumElt with
	     * matching tag, if available, and make the recursive call.  
	     *
	     * If no existing subelement in accumElt, then maybe just 
	     * insert fragElt (without it's children??) into accumElt -
	     * no have to do this based on MergeType spec
	     *
	     * for now, we guess this is the most efficient 
	     * (see 5-24-00 notes)
	     */
	    NIElement nextFragElt = fragElt.getFirstElementChild();
	    NIElement nextAccumElt;
	    while(nextFragElt != null) {
		String fragName = nextFragElt.getTagName();
		
		/* could avoid this wierd lookup by scanning merge tree 
		 * first - but that could be inefficient??
		 */
		MergeTreeNode nextMergeTreeNode = 
		    mergeTreeNode.getChildWithFragTagName(fragName);
		/* skip the fragment if there is no merge info for it */
		if(nextMergeTreeNode != null) {

		    String accumName = nextMergeTreeNode.getAccumTagName();
		
		    /* What if multiple children match??
		     * In case where nextMergeTreeNode.resultType()==scalar, 
		     * then there will be at most one accumElt child with 
		     * name accumName.  In case where 
		     * nextMergeTreeNode.resultType() == set, there
		     * may be many children of accumElt with accumName - need
		     * to call a match to figure out which one matches - we
		     * get the match out of some hash table-maintained where??
		     * (NOTE we have this exact same problem whether we scan
		     * mergeTree or fragTree first)
		     * In the case of resultType() == union, may be multiple
		     * 
		     */
		    
		    if(nextMergeTreeNode.hasMatchTemplate()) {
			/* matching is based on a match template  -
			 * may be multiple elements with accumName
			 * in accumulator, but can't be multiple
			 * matching ones by definition of accumulate -
			 * could be if it were merge
			 */
			/*
			 * Multiple frags can match to one accum and there
			 * might be multiple accum Children with tag name
			 * same as nextFragTagName, but
			 * that doesn't cause a set result from findMatch
			 */
			nextAccumElt = nextMergeTreeNode.
			    findUniqueMatch(accumElt, nextFragElt, false);
		    } else {
			/* matching is based on tag name - by
			 * definition of accumulate - there can only
			 * be one element with the tag name accumName
			 * in the accumulator - this isn't true for merge 
			 */
			nextAccumElt = accumElt.getChildByName(accumName);
		    }
		    
		    /* have to handle the case when we start with an empty
		     * accumulator - in this case nextAccumElt is null, so
		     * we make an empty element as a holder and add it
		     * to the accumulator tree 
		     */
		    if(nextAccumElt == null) {
			nextAccumElt = accumDoc.
			    createNIElement(nextMergeTreeNode.
					    getAccumTagName());
			accumElt.appendChild(nextAccumElt);
		    } else {
			nextAccumElt = nextAccumElt.makeWriteable();
		    }
		    
		    /* the recursive call */
		    do_accumulate(nextAccumElt, nextFragElt, 
				  nextMergeTreeNode, accumDoc);
		} else {
		    /*System.out.println("Found no merge info for " + fragName); */
		}
		/* now process the next sibling element */
		nextFragElt = nextFragElt.getNextElementSibling();
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

    public void dump(PrintStream os) {
	os.print("Merge Tree");
	if(validForAccumulate) {
	    os.print("(accumulate): ");
	}
	os.print("MergeType:");
	switch(treeMergeType) {
	case MT_OUTER:
	    os.print("outer");
	    break;
	case MT_LEFTOUTER:
	    os.print("leftouter");
	    break;
	case MT_RIGHTOUTER:
	    os.print("rightouter");
	    break;
	case MT_INNER:
	    os.print("inner");
	    break;
	default:
	    throw new PEException("Invalid merge type");
	}
	os.print(", Dominant Side:");
	switch(treeDomSide) {
	case DS_LEFT:
	    os.print("left");
	    break;
	case DS_RIGHT:
	    os.print("right");
	    break;
	case DS_EXACTMATCH:
	    os.print("exactmatch");
	    break;
	default:
	    throw new PEException("Invalid dominant side");
	}
	os.println();
	
	mergeTreeRoot.dump(os);
	os.println("End MergeTree");
	return;
    }

    public String toString() {
	String myString = "Merge Tree ";

	if(validForAccumulate) {
	    myString +="(accumulate) ";
	}

	myString += "Root Result Name: ";
	myString += mergeTreeRoot.getResultTagName();
	return myString;
    }
}






