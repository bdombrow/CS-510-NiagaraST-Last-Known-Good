package niagara.utils;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A <code> DOMHelper </code> class which implements some functions
 * which I wish were in the DOM specification - getting children by
 * name (instead of all descendents) and getting the first child which
 * is an element and so on. Perhaps I should put this in ndom??
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import org.w3c.dom.*;
import java.util.*;

public class DOMHelper {

    /* this class should never be instantiated */
    private void DOMHelper() {}

    /**
     * Returns the first child that is of type element. This and function
     * below give the ability to iterate through the children of type element
     *
     * @param parent The parent whose children we will search and return
     *
     * @return Returns the first child of 'parent' that is an element.
     */
    public static Element getFirstChildElement(Element parent) {
	Node child = parent.getFirstChild();

	while(child != null && child.getNodeType() != Node.ELEMENT_NODE) {
	    child = child.getNextSibling();
	}

	return (Element)child;
    }

    /**
     * Returns the next sibling that is of type element. 
     *
     * @param me We find 'me's next sibling of type element.
     *
     * @return Returns the next sibling element.
     */
    public static Element getNextSiblingElement(Element me) {
	Node sibling = me.getNextSibling();
	while(sibling != null && sibling.getNodeType() != Node.ELEMENT_NODE) {
	    sibling = sibling.getNextSibling();
	}
	/* hmm, I wonder if this will work if child is null... */
	return (Element) sibling;
    }

    /**
     * Returns all children with the specified tag name. This differs
     * from getElementsByTagName in that it returns only children
     * elements, not all descendent elements
     *
     * @param parent The parent whose children are to be searched
     * @param tagName The tag name to be used to select child elements
     *
     * @return Returns an ArrayList with the appropriate children elements
     */
    public static ArrayList getChildElementsByTagName(Element parent, 
						      String tagName) {
	NodeList nl = parent.getChildNodes();
	int size = nl.getLength();
	
	ArrayList returnList = null;
	
	/* iterate through the list and find the all the children with the 
	 * given tag name
	 */
	for (int i = 0; i < size; i++){
	    Node child = nl.item(i);
	    if(tagName.equals(child.getNodeName()) && 
	       child.getNodeType() != Node.ELEMENT_NODE) {
		if(returnList == null) {
		    returnList = new ArrayList();
		}
		returnList.add((Element)child);
	    }
	}
	/* return whatever I've found */
	return returnList;
    }

    /**
     * Retrieves the first child with tag name tagName
     *
     * @param parent The element whose children to search
     * @param tagName The tag name to search for
     *
     * @return The child with the specified tag name.
     *
     */
    public static Element getFirstChildEltByTagName(Element parent, 
						 String tagName) {
	NodeList nl = parent.getChildNodes();
	int size = nl.getLength();
	
	/* iterate through the list and find the first child with the
	 * given tag name
	 */
	for (int i = 0; i < size; i++){
	    Node child = nl.item(i);
	    if(tagName.equals(child.getNodeName()) && 
	       child.getNodeType() != Node.ELEMENT_NODE) {
		return (Element)child;
	    }
	}
	return null;
    }

    /**
     * Returns the first child of the given node which is a text
     * element 
     *
     * @param node The element to return the child of
     *
     * @return The first text child of node
     */
    public static Text getTextChild(Node node) {
	Node child = node.getFirstChild();
	while(child != null && child.getNodeType() != Node.TEXT_NODE) {
	    child = child.getNextSibling();
	}
	/* hmm, I wonder if this will work if child is null... */
	return (Text) child;
    }

}


