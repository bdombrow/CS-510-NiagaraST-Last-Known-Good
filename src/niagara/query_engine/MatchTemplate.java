package niagara.query_engine;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

import java.util.*;
import org.w3c.dom.*;
import java.io.*;
import java.lang.reflect.*;

import niagara.utils.*;
import niagara.utils.type_system.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.xmlql_parser.syntax_tree.re_parser.*;

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

    /* localKeySpec contains the list of paths (matchInfo objects) which make
     * up the local key for elements assigned to this MatchTemplate.
     * Each element in the list contains match info consisting of:
     * a path (to an elt or an attribute), matching type (tag existence, 
     * content), and a value type (int, float, etc) - type to be used 
     * for comparison.
     * The matching info can be used to generate a list/array of values
     * that are the "key" for the element.
     *
     * NOTE - must be set during initialization!!
     */
    private ArrayList localKeySpec;

    public static final int TAG_EXISTENCE = 1;
    public static final int CONTENT = 2;

    /** 
     * Constructor 
     */
    MatchTemplate() {
	localKeySpec = null;
    }

    /** 
     * Converts a MergeTemplate Element from the DOM MergeTree to
     * an in memory MatchTemplate object
     *
     * Match info contains one entry for each possible tag name
     * of a child of this parent - value of entry is the MatchInfo
     * object for children with that tagName
     * args and structure may be wrong, but need to somehow
     * initialize the 
     *
     * @param tagName The tag name of the element to which this match
     *                   template applies
     * @param matchTempl An XML element containing the match template 
     *                     specifying how to "match"
     *                     two elements of this type
     *
     */
    void readXMLTemplate(String tagName, Element matchTempl) 
	throws MTException {
	if(localKeySpec != null) {
	    throw new PEException("creating a second local key spec for a match template!!!");
	}
	localKeySpec = new ArrayList();

	Element matchNode = DOMHelper.getFirstChildElement(matchTempl);
	while(matchNode != null) {
	    localKeySpec.add(createMatchInfo(matchNode));
	    matchNode = DOMHelper.getNextSiblingElement(matchNode);
	}
	return;
    }

    private MatchInfo createMatchInfo(Element matchNode) 
	throws MTException {

	int mergeType;
	NodeHelper nodeHelper;

	/* DTD guarantees exactly one path node and exactly one
	 * content or existence node
	 */
	Element pathNode = DOMHelper.getFirstChildElement(matchNode);
	Element contentNode = DOMHelper.getNextSiblingElement(pathNode);

	regExp path = createPath(pathNode);

	if(contentNode != null && contentNode.getTagName().equals("Content")) {
	    mergeType = MatchTemplate.CONTENT;
	    
	    String type = (contentNode).getAttribute("ValueType");
	    nodeHelper = NodeHelpers.getAppropriateNodeHelper(type);
	} else {
	    mergeType = MatchTemplate.TAG_EXISTENCE;
	    nodeHelper = null;
	}

	return new MatchInfo(mergeType, path, nodeHelper);
    }

    /* take a path specified as a string and convert it to a regular
     * expression
     */
    private regExp createPath(Element pathNode) 
	throws MTException {
	String pathString = null;
	try {
	    pathString = DOMHelper.getTextChild(pathNode).getNodeValue();
	    
	    Scanner scanner;
	    regExp pathExp = null;
	    scanner = new Scanner(new StringReader(pathString));
	    REParser rep = new REParser(scanner);
	    pathExp = (regExp) rep.parse().value;
	    rep.done_parsing();
	    
	    return pathExp;
	} catch (Exception e) {
	    throw new MTException("Error parsing path String " + pathString + "  " + e.getMessage());
	}
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
    RootedKeyValue createRootedKeyValue(RootedKeyValue parentKeyVal, 
					Element elt) 
	throws OpExecException {
	return createRootedKeyValue(parentKeyVal, elt, elt.getTagName());
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
    RootedKeyValue createRootedKeyValue(RootedKeyValue parentKeyVal, 
					Element elt, String tagName) 
	throws OpExecException {
	/* get the key values from the "matching" element
	 */
	ArrayList localKeyValue = new ArrayList();
	localKeyValue.add(tagName);
	appendLocalKeyValue(elt, localKeyValue);

	RootedKeyValue newKeyVal = (RootedKeyValue)parentKeyVal.clone();

	newKeyVal.appendLocalKeyValueAndTag(localKeyValue);
	return newKeyVal;
    }

    /**
     * Appends the local key value of the given element - as determined by
     * the local key specification contained in this match
     * template - to the valueList passed as a param
     *
     * @param elt the element for which the value list is to be generated
     * @param valueList Append the local key value to this list
     *
     * @return An list of list of "key values" for this element
     */
    private void appendLocalKeyValue(Element elt, ArrayList valueList) 
	throws OpExecException {
	Vector v;

	/* localKeySpec is an array list of MatchInfo objects */
	ListIterator lkIter = localKeySpec.listIterator();

	/* iterate over local key spec - get appropriate value and insert
	 * into return list 
	 */
	while(lkIter.hasNext()) {
	    MatchInfo matchInfo = (MatchInfo)lkIter.next();
	    v = PathExprEvaluator.getReachableNodes(elt, matchInfo.path());
	    
	    /* do not allow matches on set values - vector must
	     * have length one. If length is zero, document
	     * is not key-complete.
	     */
	    if(v.size() != 1) {
		if(v.size() == 0) {
		    throw new OpExecException("Document is not key-complete");
		} else {
		    throw new OpExecException("Matches not allowed on set values");
		}
	    } else {
		Node node = (Node)v.elementAt(0);

		switch(matchInfo.mergeType()) {
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
		    valueList.add(matchInfo.nodeHelper.valueOf(node));
		    break;
		default:
		    throw new PEException("Invalid merge type");
		}
	    }
	}
	return;
    }

    public void dump(PrintStream os) {
	os.println("Match Template");
    }

}


