// $Id: LocalKey.java,v 1.12 2003/08/01 17:29:25 tufte Exp $
package niagara.query_engine;

import org.w3c.dom.*;
import java.io.*;

import niagara.utils.*;
import niagara.utils.type_system.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * Implementation of a <code> MatchTemplate </code> class whose
 * primary purpose is to take a set of xml elements and an XML
 * element and find the element in the set that matchs it. 
 */

// hi dad 

/* Notes - expand PathExprEvaluator to handle returning attributes
 * should be easy - just implement the case dataType.ATTR in
 * if(rExp instance of regExpDataNode)
 *
 * Also need a function to take a path and convert it into
 * a regExp tree.
 */

public class LocalKey {

    /* localKeySpec contains the list of paths (AtomicKey objects) which make
     * up the local key for elements assigned to this MatchTemplate.
     * Each element in the list contains match info consisting of:
     * a path (to an elt or an attribute), matching type (tag existence, 
     * content), and a value type (int, float, etc) - type to be used 
     * for comparison.
     * The matching info can be used to generate a list/array of values
     * that are the "key" for the element.
     *
     * If localKeySpec is null, this indicates that the tag is the key
     *
     * NOTE - must be set during initialization!!
     */
    private ArrayStack localKeySpec;
    private NodeVector reachableNodes;

    public static final int TAG_EXISTENCE = 1;
    public static final int CONTENT = 2;
    public static final String VALUE_SEPARATOR = ":";
    public static final String PARENT_SEPARATOR = "::";

    /** 
     * Constructor 
     */
    LocalKey() {
	localKeySpec = null;
	reachableNodes = new NodeVector();
    }

    /** 
     * Converts a MergeTemplate Element from the DOM MergeTree to
     * an in memory MatchTemplate object
     *
     * Match info contains one entry for each possible tag name
     * of a child of this parent - value of entry is the AtomicKey
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
    void readXMLMatchTemplate(String tagName, Element matchTempl) 
	throws MTException {
	if(localKeySpec != null) {
	    throw new PEException("creating a second local key spec for a match template!!!");
	}
	localKeySpec = new ArrayStack();

	Element matchNode = DOMHelper.getFirstChildElement(matchTempl);
	while(matchNode != null) {
	    localKeySpec.push(createAtomicKey(matchNode));
	    matchNode = DOMHelper.getNextSiblingElement(matchNode);
	}
	return;
    }


    /** 
     * Creates a default Local Key which is to be used
     * when no match template is provided in the Merge Template.
     * We need this default because we need to be 
     * able to create a rooted key value even when a match
     * template wasn't provided
     */
    void setTagAsKey() {
	if(localKeySpec != null) {
	    throw new PEException("setting tag as key, but localKeySpec is not null!!!");
	}
	localKeySpec = null;
	return;
    }

    private AtomicKey createAtomicKey(Element matchNode) 
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
	    mergeType = LocalKey.CONTENT;
	    
	    String type = (contentNode).getAttribute("ValueType");
	    nodeHelper = NodeHelpers.getAppropriateNodeHelper(type);
	} else {
	    mergeType = LocalKey.TAG_EXISTENCE;
	    nodeHelper = null;
	}

	return new AtomicKey(mergeType, path, nodeHelper);
    }

    /* take a path specified as a string and convert it to a regular
     * expression
     */
    private regExp createPath(Element pathNode) 
	throws MTException {
	String pathString = null;
	try {
	    pathString = DOMHelper.getTextValue(pathNode);
	    
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
     * Creates a local key value for an element. Will search the
     * element for key values as described by local key spec in
     * matcher and will create a string for the local key value
     *
     * @param elt The element whose local key should be appended to parent
     *          key value
     * @param tagName The tag name to annotate the local key value with,
     *                due to support for renaming, this sometimes
     *                is not the same as the tag name of the element
     *                 uugh, this is UGLY - I don't like it
     * @param localKeyVal modifies this object to be the local key val
     *
     * @return  
     */
    void createLocalKeyValue(Element elt, String tagName, 
			     MyStringBuffer localKeyVal) 
	throws ShutdownException {

	//localKeyVal.quickReset();
	localKeyVal.setLength(0); // reset for safety

	/* tag is always part of the key */
	//localKeyVal.push(tagName); 
	localKeyVal.append(tagName);

	/* case one - localKeySpec is null indicating the tag is the key*/
	if(localKeySpec == null) {
	    return;
	} 
	
	/* localKeySpec is an array list of AtomicKey objects */
	/* in this function, we do not allow multiple matches on
	 * a path, so we don't care how many paths in localKeySpec,
	 * just use them all
	 */
	int numPaths = localKeySpec.size();
	
	/* add one value for each path in the local key */
	for(int i = 0; i< numPaths; i++) {
	    AtomicKey atomicKey = (AtomicKey)localKeySpec.get(i);
	    if(i == 0 && numPaths == 1 && atomicKey.isNever()) {
		//localKeyVal.pop(); // removes tag??
		//localKeyVal.push("NEVER");
		localKeyVal.setLength(0); // remove tag
		localKeyVal.append("NEVER");
		return;
	    } else {
		reachableNodes.clear();
		atomicKey.getMatches(elt, reachableNodes);
		
		if(reachableNodes.size() == 0) {
		    System.out.println("The following element is not key-complete");
		    DOMHelper.printElt(elt);
		    throw new ShutdownException("Document is not key-complete. Elt tagname " + tagName + " " + elt.getTagName());
		} else  if(reachableNodes.size() != 1) {
		throw new ShutdownException("Matches not allowed on set values when path is not singular. tagname " + tagName);
		}
		// localKeyVal.push(getKeyValue(atomicKey,
		//                              (Node)reachbleNodes.get(0)));
		localKeyVal.append(VALUE_SEPARATOR);
		appendKeyValue(atomicKey, (Node)reachableNodes.get(0),
			       localKeyVal);
	    }
	}
	return;
    }

    /**
     * Creates a local key value for an element. Will search the
     * element for key values as described by local key spec 
     * and will concatenate those to create a local key value string.
     * Allows multiple matches to the local key spec (local key must
     * contain only one path in that case) in this element and returns
     * a stack/array of strings with those values
     *
     * @param elt The element whose local key should be created
     *
     * @param tagName The tag name to annotate the local key value with,
     *                due to support for renaming, this sometimes
     *                is not the same as the tag name of the element
     *                 uugh, this is UGLY - I don't like it
     * @param localKeyValList - this objet will be modified to be the 
     *         stack of local key values - each local key value
     *         is an array of objects
     *
     * @return 
     */
    public void createLocalKeyValues(Element elt, String tagName,
				     ArrayStack localKeyValList)
	throws ShutdownException {
	
	localKeyValList.quickReset();

	/* localKeySpec is an array list of AtomicKey objects */
	int numPaths = -1;
	if(localKeySpec != null)
	    numPaths = localKeySpec.size();

	if(localKeySpec == null || numPaths > 1) {
	    Object temp = localKeyValList.getAllowExpand(0);
	    MyStringBuffer localKeyVal;
	    if(temp == null || !(temp instanceof MyStringBuffer)) {
		localKeyVal = new MyStringBuffer();
	    } else {
		localKeyVal = (MyStringBuffer) temp;
	    }
	    localKeyVal.setLength(0);
	    
	    createLocalKeyValue(elt, tagName, localKeyVal);
	    localKeyValList.push(localKeyVal);
	    return;
	}	
	
	/* case two - localKey has only one path in it - allow multiple
	 * matches on this path and generate multiple local keys
	 */
	if(numPaths == 1) {
	    /* returns array/stack of array of string refs */
	    AtomicKey atomicKey = (AtomicKey)localKeySpec.get(0);
	    reachableNodes.clear();
	    atomicKey.getMatches(elt, reachableNodes);
	    
	    int numNodes = reachableNodes.size();
	    if(numNodes == 0) {
		throw new ShutdownException("Document is not key-complete. Elt tagname " + tagName);
	    }
	    
	    for(int i =0; i<numNodes; i++) {
		Object temp = localKeyValList.getAllowExpand(i);
		MyStringBuffer localKeyVal;
		if(temp == null || !(temp instanceof MyStringBuffer)) {
		    localKeyVal = new MyStringBuffer();
		} else {
		    localKeyVal = (MyStringBuffer) temp;
		}
		localKeyVal.setLength(0);

		/* the tag is always part of the key */
		localKeyVal.append(tagName);
		localKeyVal.append(VALUE_SEPARATOR);
		appendKeyValue(atomicKey,
			       (Node)reachableNodes.get(i),
			       localKeyVal);
		localKeyValList.push(localKeyVal);
	    }
	    return;
	}

	throw new PEException("Shouldn't get here!!!");
    }

    private void appendKeyValue(AtomicKey atomicKey, Node node, 
				MyStringBuffer localKeyVal) {

	switch(atomicKey.mergeType()) {
	case LocalKey.TAG_EXISTENCE:
	    localKeyVal.append(node.getNodeName());
	    return;
	    //return node.getNodeName();
	    
	case LocalKey.CONTENT:
	    /* create the appropriate type of object -
	     * Double, Integer, String, Date, with
	     * the value from the element
	     */		    
	    /* the node helper converts the node (actually
	     * node.getNodeValue()) into an instance
	     * of the appropriate type, we append the string
	     * representation of this value - this will give
	     * us a standard string for each value so even if
	     * initial values 1.0 and 1.0000 would give
	     * equivalent key values
	     */
	    localKeyVal.append(atomicKey.nodeHelper().valueOf(node));
	    return;
	default:
	    throw new PEException("Invalid merge type");
	}
    }

    public void dump(PrintStream os) {
	os.println("Local Key");
    }

    public boolean isNever() {
	if(localKeySpec == null)
	    return false;
	if(localKeySpec.size() == 1) {
	    if(((AtomicKey)(localKeySpec.get(0))).isNever())
		return true;
	}
	return false;
    }

    public boolean isTag() {
	if(localKeySpec == null) 
	    return true;
	else
	    return false;
    }
}


