package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import java.util.*;

import niagara.utils.*;
import niagara.utils.nitree.*;

/**
 * Implementation of a <code> MatchTemplate </code> class whose
 * primary purpose is to take a set of xml elements and an XML
 * element and find the element in the set that matchs it. 
 */

/* Notes - expand PathExprEvaluator to handle returning attributes
 * should be easy - just implement the case dataType.ATTR in
 * if(rExp instance of regExpDataNode)
 *
 * Also need a function to take a path and convert it into
 * a regExp tree.
 */

public class MatchTemplate {

    /* Match info contains one entry for each possible tag name
     * of a child of this parent - value of entry is the MatchInfo
     * object for children with that tagName
     * NOTE - must be set during initialization!!
     */
    private HashMap matchInfo;

    /* Hash map containing the children of currentParent - contains
     * only those children with tags listed in the tag list
     * These variables used for processing
     */
    private HashMap children;
    private HashSet tagList;
    private NIElement currentParent;
    

    public static final int TAG_EXISTENCE = 1;
    public static final int CONTENT = 2;

    /* list - indexed by, hashed on tag name - of what matching means
     * for that tag - matching info consists of a path (to an elt or
     * an attribute), matching type (tag existence, content,), and
     * a value type (int, float, etc) - type to be used for comparison.
     * The matching info can be used to generate a list/array of values
     * that are the "key" for the element.
     */

    /** 
     * Constructor 
     */
    MatchTemplate() {
	children = new HashMap();
	tagList = new HashSet();
	matchInfo = null;
	currentParent = null;
    }

    /**
     * Initialize the MatchTemplate - may be done multiple times -
     * that is a MatchTemplate instance may be reused
     *
     * Match info contains one entry for each possible tag name
     * of a child of this parent - value of entry is the MatchInfo
     * object for children with that tagName
     * args and structure may be wrong, but need to somehow
     * initialize the 
     */
    void initialize(HashMap _matchInfo) {
	reset();
	matchInfo = _matchInfo;
    }

    /** 
     * keep the match info, but get rid of all the parent-specific
     * info
     */
    void reset() {
	children.clear();
	tagList.clear();
	if(currentParent != null) {
	    currentParent.unsetMatcher();
	}
	currentParent = null;
    }
    
    /**
     * Finds the only child of parent which "matches" toMatch
     * If there are multiple matches among parent's children,
     * this is an error.
     *
     * @param parent Parent element - look for matches among this 
     * element's children
     * @param toMatch Element to be matched to - look for matches of
     *  parent's children to this element. 
     *
     * @return Returns matching element which is a child of parent.
     */
    NIElement findUniqueMatch(NIElement parent, NIElement toMatch) {
	/* if necessary, create a new hash table */
	if(parent != currentParent) {
	    buildHashMap(parent, toMatch.getTagName());
	}

	/* if necessary, add children with toMatch's tag name to
	 * the hash map
	 */
	if(!tagList.contains(toMatch.getTagName())) {
	    addToHashMap(parent, toMatch.getTagName());
	}

	/* get the "key" (valueList) and 
	 * probe the hash table for a matching object 
	 */
	ArrayList mi =(ArrayList)matchInfo.get(toMatch.getTagName());
	return (NIElement)children.get(createValueList(toMatch, mi));
    }

    /*
     * Match on:
     *   content value (of element or subelement)
     *   attribute value (of element or subelement)
     *   existence of tags
     *
     * For content and attribute value, a type specification
     * is allowed so that 4.0 can be determined to be equal to 4.00
     */
    
    /**
     * buildHashMap creates a new hash table for this parent.  It
     * inserts into the hash table only children of this parent
     * whose tag name is tagName. 
     *
     * @param parent Parent element for which has table is being constructed -
     *     appropriate children of this parent will be inserted into hash map
     * @param tagName Children (of parent) are inserted into hash map only
     *     if their tag name equals this parameter
     *
     */
    private void buildHashMap(NIElement parent, String tagName) {
	reset();
	addToHashMap(parent, tagName);

	/* set currentParent ref and also indicate to parent that
	 * we have references to it's kids in our hash table
	 */
	currentParent = parent;
	parent.setMatcher(this);
	return;
    }

    /**
     * addToHashMap adds all children of an element with a given
     * tag name.
     *
     * @param parent Parent whose children are to be inserted into hash map
     * @param tagName Only children with this tag name are to be added
     *      to the hash map
     */
    private void addToHashMap (NIElement parent, String tagName) {
	/* get an iterator over all children of parent with tagname tagName */
	ListIterator childrenIter = 
	    parent.getChildrenByName(tagName).listIterator();

	while(childrenIter.hasNext()) {
	    NIElement child = (NIElement) childrenIter.next();

	    /* create a list of values which form the "key" for this
	     * element - not really a key - just a list of values such
	     * that if this list of values equals a list from another
	     * element - then those two elements are said to match 
	     */
	    ArrayList mi =(ArrayList)matchInfo.get(child.getTagName());
	    ArrayList valueList = createValueList(child, mi);
	    mi = null;

	    /* put the child and it's "key" - the value list
	     * in the hash set - we trust Java to make a good
	     * hash value out of the valueList
	     */
	    Object oldMapping = children.put(valueList, child);
	    
	    if(oldMapping != null) {
		throw new 
		    PEException("Element with that key already in HashMap");
	    }
	}

	/* when done with all kids - add the tagName to the tagname list */
	tagList.add(tagName);
    }

    /**
     * Replaces an element in the hash map - we assume that oldElt
     * and newElt have the same key.  Basically this function is
     * to support the case where oldElt is cloned to make it
     * writeable
     *
     * @param oldElt Element to be replaced
     * @param newElt New element to replace oldElt
     */
    public void replaceElement(NIElement oldElt, NIElement newElt) {

	if(oldElt.getTagName() != newElt.getTagName()) {
	    System.err.println("BARF - bad tag names in MatchTemplate.replaceElement");
	}

	/* check to see if we have elements of this tag name in
	 * the matcher, if not jut return
	 */
	if(tagList.contains(oldElt.getTagName())) {
	    /* create a list of values which form the "key" for the
	     * new element (not really a key - just a list of values such
	     * that if this list of values equals a list from another
	     * element - then those two elements are said to match)
	     */
	    ArrayList mi =(ArrayList)matchInfo.get(newElt.getTagName());
	    ArrayList valueList = createValueList(newElt, mi);
	    
	    /* put the newElt and it's "key" into the hash set */
	    Object oldMapping = children.put(valueList, newElt);
	    
	    if(oldMapping != oldElt) {
	       throw new PEException("oldElt doesn't match elt being replaced");
	    }
	}
	   
	/* else nothing to do */
	return;
    }
    
    /**
     * Takes an element as input and generates, based on some
     * matching info (paths, match_types, value_types) a list
     * of values that form the key/group by value for this
     * element
     *
     * @param elt the element for which the value list is to be generated
     * @param miList the matching information which tells which values of
     *      this element (attributes, sub-elements, tags, etc.) are 
     *      part of the matching criteria for elements of this type
     *
     * @return An ArrayList of the "matching values" or "key values"
     *         for this element
     */
    private ArrayList createValueList(NIElement elt, ArrayList miList) {
	ArrayList valueList = new ArrayList(); /* for return */
	Vector v;

	/* mi is an array list of MatchInfo objects */
	ListIterator miIter = miList.listIterator();

	/* iterate over mi - get appropriate value and insert
	 * into return list 
	 */
	while(miIter.hasNext()) {
	    MatchInfo mi = (MatchInfo)miIter.next();
	    v = PathExprEvaluator.getReachableNodes(elt, mi.path());
	    
	    /* we don't allow matches on set values (yet), so throw
	     * an error if there is more than one reachable node 
	     */
	    if(v.size() > 1) {
		throw new PEException("Matches not allowed on set values");
	    } else if(v.size() == 0) {
		valueList.add(null);
	    } else {
		/* vector size must be one - process this node */

		NINode node = (NINode) v.elementAt(0);

		switch(mi.mergeType()) {
		case MatchTemplate.TAG_EXISTENCE:
		    valueList.add(node.getNodeName());
		    break;

		case MatchTemplate.CONTENT:
		    /* create the appropriate type of object -
		     * Double, Integer, String, Date, with
		     * the value from the element
		     */		    
		    /* the node helper converts the node (actually
		     * node.getNodeValue()) into an instance
		     * of the appropriate type
		     */
		    valueList.add(mi.nodeHelper.valueOf(node));
		    break;
		default:
		    throw new PEException("Invalid merge type");
		}

	    }
	}

	return valueList;
    }
}
