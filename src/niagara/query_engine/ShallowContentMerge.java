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

import niagara.utils.nitree.*;

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
     * Constructor for ShallowContentMerge - sets up the NodeMerge
     * fields based on arguments
     *
     * @param _contentMerge the NodeMerge object to be used for the Node content
     * @param attrNames Array of attrNames associated with the attrMerges
     * @param attrMerges Array of NodeMerges for the associated attributes
     *
     */
    ShallowContentMerge(NodeMerge _contentMerge, String[] attrNames,
			NodeMerge[] attrMerges) {
	contentMerge = _contentMerge;
	attrMergesMap = new HashMap();

	if(attrNames != null && attrMerges != null) {
	    for(int i = 0; i < attrMerges.length; i++) {
		attrMerges[i].setName(attrNames[i]); /* really for debugging */
		attrMergesMap.put(attrNames[i], attrMerges[i]);
	    }
	}
	return;
    }

    boolean isDeepMerge() { return false; }

    /**
     * Changes the merge object for the node content
     * 
     * @param _contentMerge The new merge object for the content
     */
    void addContentMerge(NodeMerge _contentMerge) {
	contentMerge = _contentMerge;
	return;
    }

    /**
     * Adds or changes the merge object for attribute named attrName
     *
     * @param attrName The name of the attribute for which NodeMerge is changing
     * @param attrMerge The new NodeMerge object
     */
    void addAttrMerge(String attrName, NodeMerge attrMerge) {
	/* remove any previous NodeMerge for this attribute */
	attrMergesMap.remove(attrName);
	attrMergesMap.put(attrName, attrMerge);
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
	/* traverse through all attributes and content - if an attribute 
	 * value (or node content) matches, no action.  If a new 
	 * attribute/content is found in fragElt, it is inserted into 
	 * accumElt.  If content/attribute value doesn't match, the value 
	 * from the "dominant" side is used.
	 */

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
	throws OpExecException {

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
	throws OpExecException {

	/* first some setup */
	leftElt = lElt;
	rElt = rElt;
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
	throws OpExecException {
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

	/* join the attrMaps - join condition is equality of attribute names
	 * look for matches in the maps - if a match found - then
	 * proceed to merge the matching attributes - if no
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

	    if(nodeMerge == null) {
		rIterator.remove();
	    } else {
		/* get a matching left attribute */
		NIAttribute lAttr = (NIAttribute)
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
		    /* lAttr is null-means rAttr is a "Right Outer" attribute */
		    processOuterAttr(rAttr, nodeMerge.includeRightOuter(), 
				     rightIsResult, resultAttrs, rightElt);
		}
	    }
	}

	/* Process remaining attributes from the left side */
	Iterator lIterator = lAttrsMap.values().iterator();	
	while(lIterator.hasNext()) {
	    NIAttribute lAttr = (NIAttribute)(lIterator.next());

	    /* Ignore all attributes without any merge information  -
	     * by removing them from the appropriate attrsMap
	     */
	    NodeMerge nodeMerge = (NodeMerge)
		(attrMergesMap.get(lAttr.getName()));
	    if(nodeMerge == null) {
		rIterator.remove();
	    } else {
		/* lAttr is null-means rAttr is a "Right Outer" attribute */
		processOuterAttr(lAttr, nodeMerge.includeLeftOuter(), 
				 leftIsResult, resultAttrs, leftElt);
	    }
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


    private void mergeContent() 
	throws OpExecException {
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
				  NIElement elt) {
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
}
