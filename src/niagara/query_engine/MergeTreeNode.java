/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

/** <code> MergeTreeNode </code> class.  The merge tree is made up of 
 * <code> MergeTreeNode </code> s.  A
 * merge tree will be created from parsing the XML representation of
 * the merge template. Each MergeTreeNode contains merge and match
 * objects which do the real merging and matching work.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.util.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import java.io.*;

import niagara.utils.*;


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
    
    private boolean hasMatchTemplate;

    /*
     * METHODS
     */

    /**
     * Constructor - creates the a MergeTreeNode corresponding to 
     * mergeTemplateElement and recursively calls itself to create 
     * MergeTreeNodes for all of the children of mergeTemplateElement.
     *
     * @param eMElt ElementMerge element which contains the merge
     *              specification to be translated to this MergeTreeNode
     * @param treeMergeType The MergeType (inner, outer, etc) specified
     *              at the tree level (default for the whole tree)
     * @param treeDomSide The Dominant Side (left/right) specified at the
     *              tree level (default for the whole tree)
     * @param checkAccumConstraints true/false depending on whether we
     *         should verify that this MergeTemplate is valid for
     *         use in accumulator.  Accumulate has certain restrictions
     *         beyond those for a general merge template, if 
     *         checkAccumConstraints is true and these constraints
     *         are violated 
     */
    MergeTreeNode(Element eMElt, int treeMergeType, int treeDomSide,
		  boolean checkAccumConstraints) 
	throws MTException {

	children = new ArrayList();

	/* First get the tag names: ResultName, LInputName, RInputName
	 * These are specified as attributes
	 */
	setTagNames(eMElt, checkAccumConstraints);

	/* The first element of an ElementMerge will be either
	 * a MatchTemplate (optional) or an element describing
	 * the merge itself (ShallowContent, DeepReplace, etc.)
	 */
	Element contentMerge = null;
	if(DOMHelper.getFirstChildElement(eMElt).getNodeName().equals("MatchTemplate")) {
	    /* we have a match template */
	    Element matchTempl = DOMHelper.getFirstChildElement(eMElt);
	    matcher = new MatchTemplate();
	    matcher.readXMLTemplate(resultTagName, matchTempl);
	    hasMatchTemplate = true;
	    contentMerge = DOMHelper.getNextSiblingElement(matchTempl);
	} else {
	    hasMatchTemplate = false;
	    matcher = null;
	    contentMerge = DOMHelper.getFirstChildElement(eMElt);
	}

	/* the ElementMerge element always contains an
	 * element to describe how the merge should be done
	 */
	String mergeMethod = contentMerge.getNodeName();
	if(mergeMethod.equals("ShallowContent")) {
	    merger = new ShallowContentMerge(contentMerge, treeDomSide,
					     treeMergeType);
	} else if(mergeMethod.equals("DeepReplace")) {
	    merger = new DeepReplaceMerge(treeDomSide);
	} else if(mergeMethod.equals("Union")) {
	    throw new PEException("Union not supported");
	} else if(mergeMethod.equals("ExactMatch")) {
	    merger = new ShallowContentMerge(contentMerge, treeDomSide,
					     treeMergeType);
	} else if(mergeMethod.equals("DoNotCare")) {
	    merger = new DoNotCareMerge();
	} else {
	    throw new PEException("Invalid Merge Method");
	}

	/* The rest of the elements are MergeNodes for the children -
	 * create the children by recursively calling the constructor
	 * on appropriate child elements of ElementMerge element */
	  
	/* create the children!! */
	Element childEMElt = 
	    DOMHelper.getNextSiblingElement(contentMerge);
	while(childEMElt != null) {
	    /* put a new MergeTreeNode in the children list */
	    MergeTreeNode temp = new MergeTreeNode(childEMElt,
						   treeMergeType,
						   treeDomSide,
						   checkAccumConstraints);
	    children.add(temp);

	    /* and continue on */
	    childEMElt = DOMHelper.getNextSiblingElement(childEMElt);
	}	  

	return;

    }

    /** Internal function to set the result, lInput and rInput
     * tag names.  Does so on the basis of attributes from the
     * ElementMerge element
     * 
     * @param eMElt The DOM element representing the specification
     * of the merge for this element
     */
    private void setTagNames(Element eMElt, boolean checkAccumConstraints) 
	throws MTException {
	/* ResultName is required - if it doesn't exist is an error 
	 * This is specified in the DTD and so we shouldn't get 
	 * null ResultName attribute here
	 */
	resultTagName = eMElt.getAttribute("Name");
	
	lInputTagName = eMElt.getAttribute("LInputName");
	if(lInputTagName.equals("")) {
	    lInputTagName = resultTagName;
	}

	if(checkAccumConstraints && !(lInputTagName.equals(resultTagName))) {
	    throw new MTException("lInputName doesn't match Name - not valid for accumulate");
	}
	
	rInputTagName = eMElt.getAttribute("RInputName");
	if(rInputTagName.equals("")) {
	    rInputTagName = resultTagName;
	}

	/* if this is for an accumulate operator, set up the accumTag
	 * and fragTag names
	 */
	if(checkAccumConstraints) {
	    fragTagName = rInputTagName;
	    accumTagName = lInputTagName; /*by convention,accumulator is left*/
	}

	return;
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

    String getResultTagName() {
	return resultTagName;
    }

    /**
     * Returns whether this merge node has a match template or not
     */
    boolean hasMatchTemplate() {
	return hasMatchTemplate;
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
     * Merges two Elements together using the merger member object.
     *
     * @param lElt "left" element to be merged
     * @param rElt "right" element to be merged
     * @param doc Document with which result is to be associated
     *
     * @return returns new result element 
     */
    Element merge(Element lElt, Element rElt, Document doc) 
	throws OpExecException {
	return merger.merge(lElt, rElt, doc, resultTagName);
    }

    /**
     * Function to accumulate one Element into another. Will use
     * the merger member object to do the accumulate.
     *
     * @param accumElt Element to be accumulated into (accumulator)
     * @param fragElt Element to be accumulated into accumElt (accumulatee)
     *
     * @return Returns true or false to indicate whether recursion 
     *         should continue or not 
     */
    void accumulate(Element accumElt, Element fragElt) 
	throws OpExecException {
	merger.accumulate(accumElt, fragElt);
    }

    /**
     * Creates a key value given a parent key value. Will search the
     * element for key values as described by local key spec in
     * matcher and will append that local key to parent key value
     *
     * @param parentKey The parent key value to use in rooted key value 
     *           creation
     * @param elt The element whose local key should be appended to parent
     *          key value
     *
     * @return 
     */
    public RootedKeyValue createRootedKeyValue(RootedKeyValue parentKey, 
					       Element elt) 
	throws OpExecException {
	return matcher.createRootedKeyValue(parentKey, elt);
    }

    /**
     * Creates a key value given a parent key value. Will search the
     * element for key values as described by local key spec in
     * matcher and will append that local key to parent key value
     *
     * @param parentKey The parent key value to use in rooted key value 
     *           creation
     * @param elt The element whose local key should be appended to parent
     *          key value
     * @param tagName The tag name to annotate the local key value with,
     *                due to support for renaming, this sometimes
     *                is not the same as the tag name of the element
     *                 uugh, this is UGLY - I don't like it
     *
     * @return 
     */
    RootedKeyValue createRootedKeyValue(RootedKeyValue parentKey, Element elt,
					String tagName) 
	throws OpExecException {
	return matcher.createRootedKeyValue(parentKey, elt, tagName);
    }

    public void dump(PrintStream os) {
	os.println("Merge Tree Node: Tags: " +
		   "R:" + resultTagName + 
		   ", l:" + lInputTagName +
		   ", r:" + rInputTagName);
	merger.dump(os);
	if(hasMatchTemplate) {
	    matcher.dump(os);
	}
	for(int i=0; i<children.size(); i++) {
	    ((MergeTreeNode)children.get(i)).dump(os);
	}
    }

    public String toString() {
	String myString = "MergeTreeNode: " + resultTagName + " ";
	myString += merger.getName();
	if(hasMatchTemplate) {
	    myString += " has match";
	}
	return myString;
    }
}





