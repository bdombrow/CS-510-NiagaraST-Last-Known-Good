/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.utils.nitree;

/* Class that (theoretically) extends DOM element interface and
 * implements copy on write - initial implementation of NIElement
 * interface will be based on dom trees.
 * Should NIElement be an interface - probably - but too much
 * maintenance work to develop an interface and a class right
 * now, just write the class NIElement and do all coding
 * in terms of NIElement
 * Eventually - can convert class to a different name and
 * make NIElement the interface name - shouldn't be too
 * hard to do that - won't require too many code changes -
 * will have to look at all new NIElement calls
 *
 * permission flags info are kept in the NIDocument
 */

import java.util.*;
import org.w3c.dom.*;

import niagara.utils.*;
import niagara.query_engine.MatchTemplate;

public class NIElement extends NINode {

    /* reference to dom element associated with this NIElement */
    private Element domElement; 
    /*private BooleanHolder writeable;*/// reference to writeable bit in NIDocument 
    private int writeableIdx;
    private boolean initialized;  // indicates if dom_element ref has been set up 
    private NIDocument ownerDoc; 
    private MatchTemplate matcher; /* changes need to be propagated here */
    
     /* IMPORTANT - NIElements should be created only through the 
      * NIDocument function createNIElement() - this is because
      * the NIDocument associated with the element must set the
      * writeable field
      */

    NIElement() {
	domElement = null;
	ownerDoc = null;
	/*writeable = null; */
	writeableIdx=-1;
	matcher = null;
	initialized = false;
    }

    /* IMPORTANT - whenever a child is added to this NIElement,
     * it is important to think about whether this element
     * is part of a hash map in a MatchTemplate - and if it
     * is, the new child needs to be inserted into that
     * hash map (if it's tag name is in the tag list)
     * We have a problem - what if a "key" value of an element
     * is updated?? Can we avoid this by some checking on
     * the merge and match templates??
     *
     */

    /* FOR CONVERSION FROM DOM */
    /*void initialize(Element _domElement, BooleanHolder _writeable, 
		    NIDocument _ownerDoc) { */
    void initialize(Element _domElement, int _writeableIdx,
		    NIDocument _ownerDoc) { 

	domElement = _domElement;
	ownerDoc = _ownerDoc;
	/* writeable = _writeable */
	writeableIdx = _writeableIdx;
	initialized = true;

	Node node = domElement.getParentNode();
	if (node instanceof Document &&
	     node != ownerDoc.getDomDoc()) {
	     throw new PEException("INITIALIZATION FAILURE - WHY ME PROBLEM");
	     }
	return;
    }

    boolean isInitialized() { return initialized; }


    /* 
     * DOM defined functions 
     */

    public String getTagName() { return domElement.getTagName();}

    /**
     * Set the value of the node to a new value
     *
     * @param nodeValue The new value for the content of this element
     */
    public void setTextValue(String newValue) 
	throws NITreeException {
	verifyWriteable();
	/* first check to see if the values are equal - do not
	 * do any updates if values are equal so as to avoid
	 * unnecessary copies
	 */
	Text textChild = ElementAssistant.getTextChild(domElement);
	if(textChild ==  null) {
	    domElement.appendChild(domElement.getOwnerDocument().
				   createTextNode(newValue));
	} else if(!(textChild.getNodeValue().equals(newValue))) {
	    domElement.replaceChild(domElement.getOwnerDocument().
				    createTextNode(newValue),
				    textChild);
	}
	return;
    }

    /**
     * Retrieve the value of the text child of this node
     *
     * @return Returns the value of the node
     */
    public String getTextValue() {
	Node textNode = ElementAssistant.getTextChild(domElement);
	if(textNode != null) {
	    return textNode.getNodeValue();
	} else {
	    return null;
	}
    }

    public String getNodeName() {
	return domElement.getNodeName();
    }

    public String myGetNodeValue() {
	return getTextValue();
    }

    public void mySetNodeValue(String nodeValue) 
	throws NITreeException {
	setTextValue(nodeValue);
    }

    public NIDocument getOwnerDocument() {
	return ownerDoc;
    }

    public NIAttribute getAttributeNode(String name) {
	Attr domAttr = domElement.getAttributeNode(name);
	return toNIAttribute(domAttr);
    }

    /* NOTE - ignore matcher update for set and remove
     * on attributes - behavior is undefined if we
     * update an attribute used for matching
     */
    public void setAttribute(String name, String value) 
	throws NITreeException {
        /* make this element writeable, by copying if necessary
	 * then call the appropriate function on the domElement
	 */
	verifyWriteable();
        domElement.setAttribute(name, value);
	return;
    }

    public NIAttribute setAttributeNode(NIAttribute niAttr) 
	throws NITreeException {
        /* make this element writeable, by copying if necessary
	 * then call the appropriate function on the domElement
	 */
	verifyWriteable(); 
	Attr domAttr = domElement.setAttributeNode(niAttr.getDomAttr());
	return toNIAttribute(domAttr);
    }
    
    public NIAttribute removeAttributeNode(NIAttribute niAttr) 
	throws NITreeException {
	/* make writeable, copying if necessary, then call
	* appropriate function
	*/
	verifyWriteable();
	Attr domAttr = domElement.removeAttributeNode(niAttr.getDomAttr());

	return toNIAttribute(domAttr);
    }

    /*
     * Element functions
     */
    public NIAttribute[] getAttributeArray() {
	/* oh god, what a nightmare!! this is going to perform just
	 * like shit
	 */
	NamedNodeMap domAttrArray = domElement.getAttributes();
	NIAttribute[] niAttrArray = null;
	for(int i = 0; i < domAttrArray.getLength(); i++) {
	    niAttrArray[i] = toNIAttribute((Attr) domAttrArray.item(i));
	}
	return niAttrArray;
    }
    
    /* 
     * COULD BE ELEMENT-HELPER FUNCTIONS (no state required)
     */

    /**
     * returns the dom element associated with this NIElement
     *
     * @return Returns the dom element.
     */
    public Element getDomElement() {
	return domElement;
    }

    /**
     * Returns the first child of this NIElement which is itself an 
     * Element (excluding Text elements)
     *
     * @return The first child which is an Element.
     */ 
    public NIElement getFirstElementChild() {
	Node node = domElement.getFirstChild();

	while(!(node instanceof Element) && node != null) {
	    node = node.getNextSibling();
	}

	return ownerDoc.getAssocNIElement((Element)node);
    }

    /**
     * Returns the next child of this NIElement which is itself
     * an Element (excluding Text elements)
     *
     * @return The next sibling which is an element
     */
    public NIElement getNextElementSibling() {
	Node node = domElement.getNextSibling();

	while(!(node instanceof Element) && node != null) {
	    node = node.getNextSibling();
	}

	return ownerDoc.getAssocNIElement((Element)node);
    }

    /**
     * Retrieves the first (and presumably only) child with tag name 
     * tagName
     *
     * @param tagName The tag name to search for
     *
     * @return The child with the specified tag name.
     *
     * @exception Throws NITreeException if there are multiple
     *             children with this tag name
     */
    public NIElement getChildByName(String tagName) 
	throws NITreeException {
	NodeList nl = domElement.getChildNodes();
	int size = nl.getLength();
	
	/* iterate through the list and find the first child with the
	 * given tag name
	 */
	NIElement returnElt = null;
	for (int i = 0; i < size; i++){
	    Node node = nl.item(i);
	    if(tagName.equals(node.getNodeName()) && node instanceof Element) {
		if(returnElt == null) {
		    returnElt = ownerDoc.getAssocNIElement((Element)node);
		} else {
		    throw new NITreeException("Multiple children with given tag name detected");
		}
	    } 
	}

	return returnElt;
    }

    /**
     * Retrieves all the children with tag name tagName.
     *
     * @param tagName The tagname to look for
     * 
     * @return An <code> ArrayList </code> containing all the
     *   children with specified tag name.
     */
    public ArrayList getChildrenByName(String tagName) {
	NodeList nl = domElement.getChildNodes();
	int size = nl.getLength();
	
	ArrayList returnList = null;
	
	/* iterate through the list and find the all the children with the 
	 * given tag name
	 */
	for (int i = 0; i < size; i++){
	    Node node = nl.item(i);
	    if(tagName.equals(node.getNodeName()) && node instanceof Element) {
		if(returnList == null) {
		    returnList = new ArrayList();
		}
		returnList.add(ownerDoc.getAssocNIElement((Element)node));
	    }
	}
	/* return whatever I've found */
	return returnList;
    }

    /**
     * Replace this element with one specified as an argument -
     * will contact this element's parent and ask to have itself
     * replaced
     *
     * @param replacementElt Element that should replace this element
     */
    public void replaceYourself(NIElement replacementElt) 
	throws NITreeException {
	getParentNode().replaceChild(replacementElt, this);
    }
    
    /**
     * return the NINode which is the parent of this node
     */
    private NINode getParentNode() {
	Node node = domElement.getParentNode();
	if(node instanceof Element) {
	    return ownerDoc.getAssocNIElement((Element)node);
	} else if (node instanceof Document) {
	/*    if(node != ownerDoc.getDomDoc()) {
		throw new PEException("Why me??!!");
	    } */
	    return ownerDoc; /* this must be the parent ! */
	} else {
	    throw new PEException("Parent must be an Element or Document");
	}
    }

    /**
     * Replaces one child with another 
     * If element is writeable - just make the update, if not
     * make itself writeable and then do update
     *
     * @param oldChild Old child element - to be replaced
     * @param newChild New child element to replace oldChildElt
     *
     */
    public void replaceChild(NIElement newChild, NIElement oldChild) 
    throws NITreeException {
	/* first make sure I'm writeable */
	verifyWriteable();

	/* now do the update */
	domElement.replaceChild(newChild.getDomElement(), 
				oldChild.getDomElement());

	/* now update the matcher - we know it has references
	 * to our kids in it */
	matcher.replaceElement(oldChild, newChild);
	return;
    }

    /**
     * Adds a child to the list of children
     * If element is writeable - just make the update, if not
     * make itself writeable and then do update
     *
     * @param child Child to be added 
     *
     */
    public void appendChild(NIElement child) 
    throws NITreeException {

	/* first make sure I'm writeable */
	verifyWriteable();

	/* now do the update */
	domElement.appendChild(child.getDomElement());

	/* no need to update the matcher - only need to 
	 * update the matcher if we change something it
	 * has a pointer to
	 */

	return;
    }

    /**
     * Makes this element writeable.  First checks to see
     * if it is writeable, if not clones itself and tells
     * it's parent about the change so the parent can copy
     * itself if necessary.
     */
   public NIElement makeWriteable() 
    throws NITreeException {
        /*if(writeable.getValue() == true) { */
        if(ownerDoc.isWriteable(writeableIdx) == true) { 
            /* I'm already writeable, just return, yipee!! */
	    return this;
	}

	/* ok, now need to clone myself and my domElement and
	 * be a good kid and tell my parents about it
	 * note clone makes the necessary association in the map table
	 */
        NIElement theNewMe = cloneEltRefChildren();

	/* Get myself replaced - this will notify my parent */
	this.replaceYourself(theNewMe);

	return theNewMe;
    }
     
    /**
     * Verifies that this element is writeable - throws an exception
     * if it isn't writeable
     */
   public void verifyWriteable() 
    throws NITreeException {
       /*if(writeable.getValue() == true) { */
        if(ownerDoc.isWriteable(writeableIdx) == true) { 
	   /* I'm already writeable, just return, yipee!! */
	   return;
       } else {
	   throw new NITreeException("Attempt to write a non-writeable element");
       }
       
   }

    /**
     * Clones this node. The associated domElement is cloned.
     * The clone is shallow - children are not cloned, with
     * the exception of the text node which is cloned.
     * Attributes are copied.  The text node is cloned to
     * give the appearance of cloning the node and it's content.
     *
     * @return Returns the new NIElement
     */
    private NIElement cloneEltRefChildren() {
	/* do a shallow clone of the domElement - note that
	 * this does not clone the text of the domElement since
	 * that is contained in a Text sub-element of the domElement
	 * also note that cloned dom element doesn't have a parent!
	 * cloneNode copies attributes...
	 */ 
        Element newDomElement = (Element)domElement.cloneNode(false);

	/* now clone the element's content (text element) */
	Node textNode = ElementAssistant.getTextChild(domElement);
	if(textNode != null) {
	    Node clone = textNode.cloneNode(false);
	    newDomElement.appendChild(clone);
	}

	/* reference all the children of the clonee - what
	 * we want is a copy of the clonee's data and attributes
	 * and references to the clonee's children
	 */
	NIElement myChild = getFirstElementChild();
	if(myChild != null) {
	    Element child = myChild.getDomElement();
	    while(child != null) {
		newDomElement.appendChild(child);
		child = ElementAssistant.getNextElementSibling(child);
	    }
	}

	/* now create the new NIElement - this function will
	 * look for an associated NIElement to newDomElement - which
	 * won't exist, so it will create a new NIElement and
	 * make the appropriate association in the map table
	 */
	return ownerDoc.createNIElement(newDomElement);
    }

    private NIAttribute toNIAttribute(Attr domAttr) {
	/* do I probe or create a new one? - probe is probably cheaper */
	return ownerDoc.getAssocNIAttr(domAttr, this);
    }

    public void setMatcher(MatchTemplate _matcher) {
	matcher = _matcher;
    }

    public void unsetMatcher() {
	matcher = null;
    }
}












