/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

/**
 * The <code>ShallowMerge</code> class which merges two
 * elements by merging the values of the attributes and shallow
 * content - where the values don't match, one element will be 
 * dominant. This is like an outer join in that attributes or
 * content in one element, but not in the other appear in the
 * result.
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.util.*;
import org.w3c.dom.*;
import java.io.*;

import niagara.utils.*;
import niagara.utils.type_system.*;

class ShallowContentMerge extends MergeObject {

    /* set these up during creation */
    HashMap attrMergesMap; /* values are NodeMerge objects, keys are attrName*/
    NodeMerge   contentMerge;

    /* used during merge processing */
    private Element leftElt;
    private Element rightElt;
    private Element resultElt;
    private boolean leftIsResult;
    private boolean rightIsResult;
    private ArrayList addAttrs;
    private ArrayList changeAttrs;
    private ArrayList delAttrs;
    private HashMap procLeftAttrs;

    /**
     * Constructor for ShallowContentMerge - reads in a ShallowContent
     * DOM element from the XML merge tree 
     */
    ShallowContentMerge(Element shallowContElt, int treeDomSide,
			int treeMergeType, MergeTree mergeTree)
	throws MTException {
	if(shallowContElt.getNodeName().equals("ExactMatch")) {
	    createExactMatchMerge();
	}

	contentMerge = createNodeMerge(shallowContElt, 
				       treeDomSide, treeMergeType);
	
 	attrMergesMap = new HashMap();

	/* Go through chidren of shallowContElt and create
	 * attribute merging info
	 */
	String attrName;
	NodeMerge attrNodeMerge;
	Element attrShallowContElt;

	Element attrMergeElt = 
	    DOMHelper.getFirstChildElement(shallowContElt);
	while(attrMergeElt != null) {
	    attrName = attrMergeElt.getAttribute("Name");
	    attrShallowContElt = 
		DOMHelper.getFirstChildElement(attrMergeElt);
	    if(attrShallowContElt != null) {
		attrNodeMerge=createNodeMerge(attrShallowContElt, 
					      treeDomSide, treeMergeType);
		/* just for safety */
		assert DOMHelper.getNextSiblingElement(attrMergeElt) == null:
		   "KT: Unexpected sibling";
	    } else {
		/* attribute is listed, but no merge method specified -
		 * exactMatch is the default
		 */
		NodeHelper comparator = 
		    getAppropriateNodeHelper(shallowContElt);
		attrNodeMerge = new ExactMatchNodeMerge(comparator);
	    }
	    attrNodeMerge.setName(attrName);
	    attrMergesMap.put(attrName, attrNodeMerge);
	    attrMergeElt =
		DOMHelper.getNextSiblingElement(attrMergeElt);
	}

	this.mergeTree = mergeTree;

	addAttrs = new ArrayList(); 
	changeAttrs = new ArrayList();
	delAttrs = new ArrayList();
	procLeftAttrs = new HashMap(); 

	return;
    }

    private NodeHelper getAppropriateNodeHelper(Element shallowContElt) 
	throws MTException {
	String lValueType = shallowContElt.getAttribute("LValueType");
	String rValueType = shallowContElt.getAttribute("RValueType");
	try {
	    NodeHelper ret = NodeHelpers.getAppropriateNodeHelper(lValueType, rValueType);
	    return ret;
	} catch (MTException mte) {
	    throw new MTException("Shallow Content Merge (" 
				  + shallowContElt.getTagName() + ") " 
				  + mte.getMessage());
	}
    }

    private NodeMerge createNodeMerge(Element shallowContElt, 
				      int treeDomSide, int treeMergeType) 
	throws MTException {

	String fcn = shallowContElt.getAttribute("Function");
	NodeHelper comparator = getAppropriateNodeHelper(shallowContElt);
	NodeMerge contentMerge;

	if(shallowContElt.getTagName().equals("ExactMatch") ||
	   fcn.equals("exactMatch")) {
	    contentMerge = new ExactMatchNodeMerge(comparator);
	} else if(fcn.equals("replace")) {
	    contentMerge = new ReplaceNodeMerge(treeDomSide, treeMergeType,
						comparator);
	} else if (fcn.equals("sum") || fcn.equals("average") ||
		   fcn.equals("max") || fcn.equals("min")) {
	    assert comparator instanceof NumberNodeHelper :
		"Invalid Node Helper - need NumberNodeHelper here";
	    /* oh, forgive me for this one... */
	    if(fcn.equals("average")) {
		comparator = NodeHelpers.DOUBLEHELPER;
	    }
	    contentMerge = new AggNodeMerge(treeMergeType, 
					    (NumberNodeHelper)comparator, fcn);
	} else if (fcn.equals("noContent")) {
	    contentMerge = new NoContentNodeMerge();
	} else {
	    throw new MTException("In element " + shallowContElt.getTagName() +
				  " Fcn: " + fcn + 
	     " is an invalid shallow content function (replace | sum | average | max |  min | exactMatch)");
	}
	return contentMerge;
    }

    private void createExactMatchMerge() {
	contentMerge = new ExactMatchNodeMerge(NodeHelpers.STRINGHELPER);
	
 	attrMergesMap = new HashMap();

	String attrName = MergeTree.DEF_ATTR_STRING;
	NodeMerge attrNodeMerge = new ExactMatchNodeMerge(NodeHelpers.STRINGHELPER);

	attrNodeMerge.setName(attrName);
	attrMergesMap.put(attrName, attrNodeMerge);

	return;
    }


    boolean isDeepMerge() { return false; }

    /**
     * Changes the merge object for the node content
     * 
     * @param _contentMerge The new merge object for the content
     */
    /*
    void addContentMerge(NodeMerge _contentMerge) {
	contentMerge = _contentMerge;
	return;
    }
    */

    /**
     * Adds or changes the merge object for attribute named attrName
     *
     * @param attrName The name of the attribute for which NodeMerge is changing
     * @param attrMerge The new NodeMerge object
     */
    /*    void addAttrMerge(String attrName, NodeMerge attrMerge) {
	* remove any previous NodeMerge for this attribute *
	attrMergesMap.remove(attrName);
	attrMergesMap.put(attrName, attrMerge);
	return;
    }
    */

    /** accumulates the specified fragment element into the accumulator
     *
     * @param accumElt The element to be accumulated into (accumulator)
     * @param fragElt The element to accumulate into accumElt (accumulatee)
     *
     * @return Returns nothing.
     */
    void accumulate(Element accumElt, Element fragElt) 
	throws ShutdownException{
	/* traverse through all attributes and content - if an attribute 
	 * value (or node content) matches, no action.  If a new 
	 * attribute/content is found in fragElt, it is inserted into 
	 * accumElt.  If content/attribute value doesn't match, the value 
	 * from the "dominant" side is used.
	 */

	/* convention - accumulator is always left */
	internal_merge(accumElt, fragElt, accumElt);	
    }

    Element accumulateEmpty(Element fragElt, String accumTagName) 
	throws ShutdownException{
	/* traverse through all attributes and content - if an attribute 
	 * value (or node content) matches, no action.  If a new 
	 * attribute/content is found in fragElt, it is inserted into 
	 * accumElt.  If content/attribute value doesn't match, the value 
	 * from the "dominant" side is used.
	 */

	/* Handle the case with an empty accumElt here.
	 * This may occur when we start with a null accumulator
	 */
	Element accumElt = createNewAccumElt(accumTagName);

	/* convention - left is accumulator */
	internal_merge(null, fragElt, accumElt); 
	return accumElt;
    }

    /** 
     * merges the two elements together and returns a new merge object
     *
     * @param lElt "left" element to be merged
     * @param rElt "right" element to be merged 
     * @param resDoc The document with which the result element is to be associated
     * @param tagName The tag name for the new result element
     *
     * @return Returns new result element
     */
    Element merge(Element lElt, Element rElt, Document resDoc,
		  String tagName) 
	throws ShutdownException {

	Element resElt = resDoc.createElement(tagName);
	internal_merge(rElt, lElt, resElt);
	return resElt;
    }


    /**
     * Merges two elements together and puts the result into a pre-existing
     * result element.  This is used internally and captures both the
     * accumulate and merge functionality.
     *
     * @param lElt "left" element to be merged
     * @param rElt "right" element to be merged
     * @param resElt Result element - this elt will be modified and
     *               upon return will contain the result of the merge 
     *               Note - attrs are added to resElt and any existing
     *               attrs will be preserved (needed for accumulate)
     *
     * @return True to indicate that recursion should continue
     */
    private void internal_merge(Element lElt, Element rElt, 
				Element resElt) 
	throws ShutdownException {

	/* first some setup */
	leftElt = lElt;
	rightElt = rElt;
	resultElt = resElt;

	/* for efficiency - just do this test once - we may
	 * have one of these cases when we are merging
	 */
	leftIsResult = (resultElt == leftElt);
	rightIsResult = (resultElt == rightElt);

	/* now deal with the content 
	 */
	mergeContent();

	if(leftElt == null)
	    leftElt = resultElt;
	/* merge the attributes */
	mergeAttributes();

	return;
    }

    /**
     * Function to merge the attributes of two elements. One
     * element is always dominant - that element's content is
     * put in result and if the two elements have any of the same
     * attributes - the attribute value from the dominant element
     * is taken.  As for attributes that appear in one element, but
     * not in the other, several options are supported - left outer
     * "join", right outer "join", inner "join", and outer "join"
     *
     * The result element is modified appropriately.
     *
     * Maybe should turn this into a HashJoin class...
     */
    private void mergeAttributes() 
	throws ShutdownException {
	/* create hash maps from the attributes - I don't use
	 * NamedNodeMap because I don't want attributes with
	 * default values to be replaced, and because I don't
	 * want to change the state of leftElt and rightElt
	 * Questionable - would like to leave accumAttrsMap
	 * around in accum case...
	 */
	NamedNodeMap lAttrsMap = leftElt.getAttributes();
	NamedNodeMap rAttrsMap = rightElt.getAttributes();

	/* array of attributes to be added to, changed in and
	 * delete from the result element */
	addAttrs.clear();
	changeAttrs.clear();
	delAttrs.clear();

	/* left attrs which have been processed */
	procLeftAttrs.clear();

	int numLAttrs = lAttrsMap.getLength();
	int numRAttrs = rAttrsMap.getLength();

	if(numLAttrs == 0 && numRAttrs == 0) {
	    /* nothing to do - there are no attributes */
	    return;
	} else if (numLAttrs == 0) {
	    /* only attributes on "right" */
	    processAttrsAsOuter(rAttrsMap, false, rightIsResult,
				addAttrs, delAttrs);
	} else if (numRAttrs == 0) {
	    processAttrsAsOuter(lAttrsMap, true, leftIsResult,
				addAttrs, delAttrs);
	} else {
	    /* join the attrMaps - join condition is equality of 
	     * attribute names look for matches in the maps - if a match 
	     * found - then proceed to merge the matching attributes - if no
	     * match found, process the "outer" attribute appropriately
	     */

	    /* iterate through right(fragment) and probe left (accum)
	     */
	    for(int i=0; i<numRAttrs; i++) {
		Attr rAttr = (Attr)(rAttrsMap.item(i));
		
		/* Ignore all attributes without any merge information  -
		 * by removing them from the appropriate attrsMap
		 */
		NodeMerge nodeMerge = (NodeMerge)
		    (attrMergesMap.get(rAttr.getName()));

		/* handle the default (ATTR_DEFAULT).  If a default
		 * is provided, it applies to all attributes that haven't
		 * had AttributeMerge elements explicitly specified.
		 */
		if(nodeMerge == null) {
		    nodeMerge = (NodeMerge)(attrMergesMap.
					    get(MergeTree.DEF_ATTR_STRING));
		}
		
		if(nodeMerge == null) {
		    if(rightIsResult) {
			delAttrs.add(rAttr);
		    } /* else do nothing, attr wont end up in result */
		} else {
		    /* get a matching left attribute */
		    Attr lAttr = null;
		    lAttr = (Attr)
			(lAttrsMap.getNamedItem(rAttr.getName()));
		    
		    if(lAttr != null) {
			/* attribute names are the same, so we have the same
			 * attribute, so merge the matching attributes 
			 */
			Attr tempResultAttr = 
			    resultElt.getOwnerDocument().createAttribute(lAttr.getName());
			boolean new_result;
			new_result = nodeMerge.merge(lAttr, rAttr, 
						     tempResultAttr);
		
			if(new_result) {
			    if(rightIsResult || leftIsResult){
				changeAttrs.add(tempResultAttr);
			    } else {
				addAttrs.add(tempResultAttr);
			    }
			}
		    
			/* keep a list of the left attrs that have been
			 * processed
			 */ 
			procLeftAttrs.put(lAttr.getName(), lAttr);
		    } else {
			/* lAttr is null-means rAttr is a 
			 * "Right Outer" attribute */
			processOuterAttr(rAttr, nodeMerge.includeRightOuter(), 
					 rightIsResult, addAttrs, delAttrs); 
		    }
		}
	    }

	    /* Process remaining attributes from the left side 
	     * an attribute is "remaining" if it hasn't already
	     * been processed and put in the procLeftAttrs list
	     */
	    int len = lAttrsMap.getLength();
	    for(int i=0; i < len; i++) {
		Attr attr = (Attr)lAttrsMap.item(i);

		/* Ignore all attributes without any merge information  -
		 * by not doing anything
		 */
		NodeMerge nodeMerge = (NodeMerge)
		    (attrMergesMap.get(attr.getName()));
		if(nodeMerge != null) {
		    if(procLeftAttrs.get(attr.getName()) == null) {
			processOuterAttr(attr, nodeMerge.includeLeftOuter(), 
					 leftIsResult, addAttrs, delAttrs);
		    }
		}
	    }
	}

	/* add the new result attributes to the result Elemement */
	int len = addAttrs.size();
	for(int i=0; i < len; i++) {
	    Attr addedAttr = (Attr)(addAttrs.get(i));
	    Attr toAddAttr = (Attr)(mergeTree.accumDoc.importNode(addedAttr, false));
	    resultElt.setAttributeNode(toAddAttr);
	}

	/* delete the deleted attributes from the result Element */
	len = delAttrs.size();
	for(int i=0; i < len; i++) {
	    resultElt.removeAttributeNode((Attr)(delAttrs.get(i)));
	}

	len = changeAttrs.size();
	for(int i=0; i<len; i++) {
	    Attr chgdAttr = (Attr)changeAttrs.get(i);
	    Attr a;
	    a = resultElt.getAttributeNode(chgdAttr.getName());
	    /* a wont be null, since to change, we know we
	     * had attr values from both sides
	     */
	    if(a == null)
		System.out.println("Chgd attr name:value " + chgdAttr.getName() +":"+
				   chgdAttr.getValue());
	    a.setValue(chgdAttr.getValue());
	}

	return;
    }

    /**
     * Process all attributes in a map from a side. For some reason
     * we know that these attrs do not have a match on the other side
     * - this can be because the other attrsMap is null or because
     * all matching attrs have already been processed
     *
     * @param attrsMap The hash map of the attributes to be processed
     */
    private void processAttrsAsOuter(NamedNodeMap attrsMap, boolean isLeft,
				     boolean isResult, 
				     ArrayList resultAttrs, 
				     ArrayList deletedAttrs) {
	/* Process remaining attributes from the given side */
	int numAttrs = attrsMap.getLength();
	for(int i = 0; i<numAttrs; i++) {
	    Attr attr = (Attr)(attrsMap.item(i));
		
	    /* Ignore all attributes without any merge information  -
	     * by not doing anything
	     */
	    NodeMerge nodeMerge = (NodeMerge)
		(attrMergesMap.get(attr.getName()));
	    if(nodeMerge != null) {
		boolean includeOuter;
		if(isLeft) {
		    includeOuter =  nodeMerge.includeLeftOuter();
		} else {
		    includeOuter = nodeMerge.includeRightOuter();
		}
		/* lAttr is null-means attr is a 
		 * "1Outer" attribute */
		processOuterAttr(attr, includeOuter, isResult,
				 resultAttrs, deletedAttrs);
	    } 
	}
    }
    

    private void mergeContent() 
	throws ShutdownException {
	contentMerge.merge(leftElt, rightElt, resultElt);
	return;
    }

    /**
     * Create a hash map from an array of attributes - the key is
     * the attribute name, the value is the attribute itself
     *
     * @param attributeArray The array of attributes to put into the hash map
     *
     * @return The newly-created hash map for the attributes.
     */
    private HashMap createAttrsHashMap(Attr[] attributeArray) {
	if(attributeArray == null) {
	    return null;
	}

	HashMap attrsHM = new HashMap();
	int len = attributeArray.length;
	for(int i=0; i < len; i++) {
	    attrsHM.put(attributeArray[i].getName(), attributeArray[i]);
	}
	return attrsHM;
    }

    /**
     * Function to process an attribute which doesn't have a match
     * in the other element. Call this an "outer" attribute as in
     * outer join terminology - which is basically what is going on here
     *
     * @param attr The attribute to be processed
     * @param includeOuter Indicates if this attr should be included
     *                     in result even if there is no matching
     *                     attr on other side
     * @param isResult Indicates if elt is the actual result elt or not
     * @param resultAttrs  The place to put any attrs that should be
     *                     added to the result (use a list to avoid
     *                     haloween problem-like effects
     * @param elt          The element associated with this attr
     *
     */
    private void processOuterAttr(Attr attr, boolean includeOuter,
				  boolean isResult, ArrayList resultAttrs,
				  ArrayList deletedAttrs) {
	/* isResult  && inclOuter - nothing to do - attr already in result
	 * isResult  && !inclOuter - remove attr from result
	 * !isResult && inclOuter - add attr to result
	 * !isResut  && !inclOuter - nothing to do - attr
	 *    isn't in result and shouldn't be
	 */
	if(!isResult && includeOuter) {
	    resultAttrs.add(attr);
	} else if (isResult && !includeOuter) {
	    deletedAttrs.add(attr);
	}
	return;
    }

    public void dump(PrintStream os) {
	os.println("Shallow Content Merge");
	contentMerge.dump(os);
	return;
    }

    public String toString() {
	return "Shallow Content Merge " + contentMerge.getName();
    }

    public String getName() {
	return "ShallowContent";
    }
}



