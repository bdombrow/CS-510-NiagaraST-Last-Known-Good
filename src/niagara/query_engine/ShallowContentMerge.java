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

import niagara.utils.nitree.*;
import niagara.utils.*;
import niagara.utils.type_system.*;

class ShallowContentMerge extends MergeObject {

    /* set these up during creation */
    HashMap attrMergesMap; /* values are NodeMerge objects, keys are attrName*/
    NodeMerge   contentMerge;

    /* used during merge processing */
    private NIElement leftElt;
    private NIElement rightElt;
    private NIElement resultElt;
    private boolean leftIsResult;
    private boolean rightIsResult;

    /**
     * Constructor for ShallowContentMerge - reads in a ShallowContent
     * DOM element from the XML merge tree 
     */
    ShallowContentMerge(Element shallowContElt, int treeDomSide,
			int treeMergeType) 
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
	    ElementAssistant.getFirstElementChild(shallowContElt);
	while(attrMergeElt != null) {
	    attrName = attrMergeElt.getAttribute("AttrName");
	    attrShallowContElt = 
		ElementAssistant.getFirstElementChild(attrMergeElt);
	    if(attrShallowContElt != null) {
		attrNodeMerge=createNodeMerge(attrShallowContElt, 
					      treeDomSide, treeMergeType);
		/* just for safety */
		if(ElementAssistant.getNextElementSibling(attrMergeElt)
		   != null) {
		    throw new PEException("Unexpected sibling");
		}
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
		ElementAssistant.getNextElementSibling(attrMergeElt);
	}

	return;
    }

    private NodeHelper getAppropriateNodeHelper(Element shallowContElt) 
	throws MTException {
	String lValueType = shallowContElt.getAttribute("LValueType");
	String rValueType = shallowContElt.getAttribute("RValueType");
	
	return NodeHelpers.getAppropriateNodeHelper(lValueType, rValueType);
    }

    private NodeMerge createNodeMerge(Element shallowContElt, 
				      int treeDomSide, int treeMergeType) 
	throws MTException {

	String fcn = shallowContElt.getAttribute("Function");
	NodeHelper comparator = getAppropriateNodeHelper(shallowContElt);
	NodeMerge contentMerge;

	if(fcn.equals("replace")) {
	    contentMerge = new ReplaceNodeMerge(treeDomSide, treeMergeType,
						comparator);
	} else if (fcn.equals("exactMatch")) {
	    contentMerge = new ExactMatchNodeMerge(comparator);
	} else if (fcn.equals("sum") || fcn.equals("average") ||
		   fcn.equals("max") || fcn.equals("min")) {
	    if(!(comparator instanceof NumberNodeHelper)) {
		throw new PEException("Invalid Node Helper - need NumberNodeHelper here");
	    }
	    /* oh, forgive me for this one... */
	    if(fcn.equals("average")) {
		comparator = NodeHelpers.DOUBLEHELPER;
	    }
	    contentMerge = new AggNodeMerge(treeMergeType, 
					    (NumberNodeHelper)comparator, fcn);
	} else {
	    throw new MTException(fcn + " is an invalid shallow content function (sum | average | max |  min)");
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
     * @exception OpExecException Thrown if exact match criteria isn't met
     */
    void accumulate(NIElement accumElt, NIElement fragElt) 
	throws OpExecException, NITreeException {
	/* traverse through all attributes and content - if an attribute 
	 * value (or node content) matches, no action.  If a new 
	 * attribute/content is found in fragElt, it is inserted into 
	 * accumElt.  If content/attribute value doesn't match, the value 
	 * from the "dominant" side is used.
	 */

	/* Handle the case with an empty accumElt here.
	 * This may occur when we start with a null accumulator
	 */
	if (accumElt.myGetNodeValue() == null) {
	    accumElt.mySetNodeValue(fragElt.myGetNodeValue());
	    return;
	}

	/* convention - accumulator is always left */
	internal_merge(accumElt, fragElt, accumElt);	
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
    NIElement merge(NIElement lElt, NIElement rElt, NIDocument resDoc,
		    String tagName) 
	throws OpExecException, NITreeException {

	NIElement resElt = resDoc.createNIElement(tagName);
	
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
    private void internal_merge(NIElement lElt, NIElement rElt, 
				NIElement resElt) 
	throws OpExecException, NITreeException {

	/* first some setup */
	leftElt = lElt;
	rightElt = rElt;
	resultElt = resElt;

	/* for efficiency - just do this test once - we may
	 * have one of these cases when we are merging
	 */
	leftIsResult = (resultElt == leftElt);
	rightIsResult = (resultElt == rightElt);

	/* merge the attributes */
	hashMergeAttributes();

	/* now deal with the content 
	 */
	mergeContent();

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
    private void hashMergeAttributes() 
	throws OpExecException, NITreeException {
	/* create hash maps from the attributes - I don't use
	 * NamedNodeMap because I don't want attributes with
	 * default values to be replaced, and because I don't
	 * want to change the state of leftElt and rightElt
	 * Questionable - would like to leave accumAttrsMap
	 * around in accum case...
	 */
	HashMap lAttrsMap = createAttrsHashMap(leftElt.getAttributeArray());
	HashMap rAttrsMap = createAttrsHashMap(rightElt.getAttributeArray());

	/* array of attributes to be added to the result element */
	ArrayList resultAttrs = new ArrayList(); 
	ArrayList changedAttrs = new ArrayList();

	if(lAttrsMap == null && rAttrsMap == null) {
	    /* nothing to do - there are no attributes */
	    return;
	} else if (lAttrsMap == null) {
	    /* only attributes on "right" */
	    processAttrsAsOuter(rAttrsMap, false, rightIsResult,
				resultAttrs, rightElt);
	} else if (rAttrsMap == null) {
	    processAttrsAsOuter(lAttrsMap, true, leftIsResult,
				resultAttrs, leftElt);
	} else {

	    /* join the attrMaps - join condition is equality of 
	     * attribute names look for matches in the maps - if a match 
	     * found - then proceed to merge the matching attributes - if no
	     * match found, process the "outer" attribute appropriately
	     */
	    /* iterate through right(fragment) and probe left (accum)
	     */
	    Iterator rIterator = rAttrsMap.values().iterator();
	    while(rIterator.hasNext()) {
		NIAttribute rAttr = (NIAttribute)(rIterator.next());
		
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
		    rIterator.remove();
		} else {
		    /* get a matching left attribute */
		    NIAttribute lAttr = null;
		    lAttr = (NIAttribute)
			(lAttrsMap.get(rAttr.getName()));
		    
		    if(lAttr != null) {
			/* attribute names are the same, so we have the same
			 * attribute, so merge the matching attributes 
			 */
			NIAttribute tempResultAttr = new NIAttribute();
			boolean new_result;
			new_result = nodeMerge.merge(lAttr, rAttr, 
						     tempResultAttr);
			
			if((rightIsResult || leftIsResult) && new_result) {
			    changedAttrs.add(tempResultAttr);
			} else {
			    resultAttrs.add(tempResultAttr);
			}
		    
			/* remove the leftAttr from the hash map
			 * to indicate it has already been processed
			 */
			lAttrsMap.remove(lAttr.getName());
		    } else {
			/* lAttr is null-means rAttr is a 
			 * "Right Outer" attribute */
			processOuterAttr(rAttr, nodeMerge.includeRightOuter(), 
					 rightIsResult, resultAttrs, rightElt);
		    }
		}
	    }

	    /* Process remaining attributes from the left side */
	    processAttrsAsOuter(lAttrsMap, true,
				leftIsResult, resultAttrs, leftElt);
	}

	/* add the new result attributes to the result Elemement */
	int rLength = resultAttrs.size();
	for(int i=0; i< rLength; i++) {
	    resultElt.setAttributeNode((NIAttribute)(resultAttrs.get(i)));
	}

	rLength = changedAttrs.size();
	if(rightIsResult || leftIsResult) {
	    for(int i=0; i<rLength; i++) {
		NIAttribute chgdAttr = (NIAttribute)changedAttrs.get(i);
		NIAttribute a;
		a = resultElt.getAttributeNode(chgdAttr.getName());
		/* a wont be null, since to change, we know we
		 * had attr values from both sides
		 */
		a.setValue(chgdAttr.getValue());
	    }
	} else {
	    for(int i=0; i< rLength; i++) {
		resultElt.setAttributeNode((NIAttribute)(changedAttrs.get(i)));
	    }
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
    private void processAttrsAsOuter(HashMap attrsMap, boolean isLeft,
				     boolean isResult, ArrayList resultAttrs,
				     NIElement elt) 
	throws NITreeException {
	/* Process remaining attributes from the given side */
	Iterator iterator = attrsMap.values().iterator();	
	    while(iterator.hasNext()) {
		NIAttribute attr = (NIAttribute)(iterator.next());
		
		/* Ignore all attributes without any merge information  -
		 * by removing them from the appropriate attrsMap
		 */
		NodeMerge nodeMerge = (NodeMerge)
		    (attrMergesMap.get(attr.getName()));
		if(nodeMerge == null) {
		    iterator.remove();
		} else {
		    boolean includeOuter;
		    if(isLeft) {
			includeOuter =  nodeMerge.includeLeftOuter();
		    } else {
			includeOuter = nodeMerge.includeRightOuter();
		    }
		    /* lAttr is null-means attr is a 
		     * "1Outer" attribute */
		    processOuterAttr(attr, includeOuter, isResult,
				     resultAttrs, elt);
		}
	    }
	}


    private void mergeContent() 
	throws OpExecException, NITreeException {
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
    private HashMap createAttrsHashMap(NIAttribute[] attributeArray) {
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
    private void processOuterAttr(NIAttribute attr, boolean includeOuter,
				  boolean isResult, ArrayList resultAttrs,
				  NIElement elt) 
	throws NITreeException {
	/* isResult  && inclOuter - nothing to do - attr already in result
	 * isResult  && !inclOuter - remove attr from result
	 * !isResult && inclOuter - add attr to result
	 * !isResut  && !inclOuter - nothing to do - attr
	 *    isn't in result and shouldn't be
	 */
	if(!isResult && includeOuter) {
	    resultAttrs.add(attr);
	} else if (isResult && !includeOuter) {
	    elt.removeAttributeNode(attr);
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



