package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> NodeMerge </code> element will merge two nodes
 * together. Nodes may be attributes or elements - would be
 * nice if the aggregation code could handle both element
 * and attr nodes, for example
 * 
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.lang.*;
import java.lang.reflect.*;

import niagara.utils.type_system.*;
import niagara.utils.nitree.*;

class NodeMerge {
    /* Three types of node merges - 
     * 1) take the dominant content
     * 2) do some aggregation of the content 
     * 3) exact match
     */

    /* includeOuter is used to
     * specify what is to be done when an attr from left,right
     * is found not in the other side - together - they can
     * be used to specify the full join spectrum, inner, outer,
     * left outer, right outer.  Note only inner and right outer
     * make sense in the accumulate case.
     * These values not used in merge, just stored here 
     * must be set for dominantcontent and aggregate merges
     */
    private boolean includeLeftOuter;
    private boolean includeRightOuter;
    
    /* isDominant indicates which side is dominant if
     * content or attribute values are equal
     * valid for dominant content and exact match merges
     */
    private boolean leftIsDominant;

    /* both valid for only aggregate merge */
    private Method aggMethod; /* typically a NodeHelper method */
    private Object aggObject; /* on which aggMethod should be called */

    /* allowed values are EXACT_MATCH, DOMINANT_CONTENT, AGGREGATE */
    private String mergeType; 

    private NodeHelper comparator;
    private String name; /* mainly for debugging purposes */
    
    NodeMerge() {
	includeLeftOuter = false;
	includeRightOuter = false;
	leftIsDominant = false;
	aggMethod = null;
	aggObject = null;
	mergeType = null;
	comparator = null;
	name = null;
    }

    void setAsExactMatch(NodeHelper _comparator) {
	mergeType = new String("EXACT_MATCH");
	comparator = _comparator;
	includeLeftOuter = includeRightOuter = leftIsDominant = false;
	aggMethod = null;
	aggObject = null;
	return;
    }

    void setAsDominantContent(boolean _leftIsDominant, 
			      boolean _includeLeftOuter,
			      boolean _includeRightOuter,
			      NodeHelper _comparator) {
	mergeType = new String("DOMINANT_CONTENT");
	leftIsDominant = _leftIsDominant;
	includeLeftOuter = _includeLeftOuter;
	includeRightOuter = _includeRightOuter;
	comparator = _comparator;
	aggMethod = null;
	aggObject = null;

    }

    void setAsAggregate(boolean _includeLeftOuter,
			boolean _includeRightOuter,
			Method _aggMethod, Object _aggObject) {
	mergeType = new String("AGGREGATE");
	leftIsDominant = false;
	includeLeftOuter = _includeLeftOuter;
	includeRightOuter = _includeRightOuter;
	aggMethod = _aggMethod;
	aggObject = _aggObject;
	comparator = null;
    }

    /**
     * Function to merge two nodes (can be attrs or elements)
     *
     * @param lNode left node to be merged
     * @param rNode right node to be merged
     * @param resultNode - the place to put the new result
     * 
     * @return Returns true if resultNode changed, false if
     *         no updates need to be made based on this merge
     */

    boolean merge(NINode lNode, NINode rNode, NINode resultNode) 
	throws OpExecException {

	if(mergeType.equals("EXACT_MATCH")) {
	    if(!comparator.nodeEquals(lNode, rNode)) {
		throw new 
		    OpExecException("Non-matching elements in ExactMatchMerge");
	    } else {
		/* everything is OK */
		return false;
	    }
	} else if(mergeType.equals("DOMINANT_CONTENT")) {
	    NINode dominantNode = null;
	    NINode submissiveNode = null;
	    if(leftIsDominant) {
		dominantNode = lNode;
		submissiveNode = rNode;
	    } else {
		dominantNode = rNode;
		submissiveNode = lNode;
	    }
	    
	    if(dominantNode != resultNode && 
	       !comparator.nodeEquals(dominantNode, submissiveNode)) {
		resultNode.setNodeValue(dominantNode.getNodeValue());
		return true;
	    } else { 
	    /* else nothing to do - result is already dominant or
	       dominant and submissive values are the same */
		return false;
	    }
	} else if(mergeType.equals("AGGREGATE")) {
	    Object[] args = {lNode, rNode, resultNode};
	    try {
		return ((Boolean)aggMethod.invoke(aggObject, args)).booleanValue();
	    } catch (IllegalAccessException e1){
		throw new 
		    OpExecException("Invalid Method Invocation");
	    } catch (InvocationTargetException e2) {
		throw new 
		    OpExecException("Invalid Method Invocation");
	    }
	}
	return false; /* make the compiler happy */
    }
    
    /**
     * Function to set name for this node merge object
     * no real use for this other than debugging - will typically
     * be the attr name if set
     *
     * @param _name Name to be used for this <code> NodeMerge </code> object.
     */
    void setName(String _name) {
	name = _name;
    }

    /**
     * Indicates if tuples from the "left" side, with no matching
     * tuples on the "right" side should be included in result
     *
     */
    boolean includeLeftOuter() {
	return includeLeftOuter;
    }

    /**
     * Indicates if tuples from the "right" side, with no matching
     * tuples on the "left" side should be included in result
     *
     */
    boolean includeRightOuter() {
	return includeRightOuter;
    }
}

