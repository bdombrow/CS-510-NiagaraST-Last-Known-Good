/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


package niagara.utils.nitree;

/**
 * Niagara implementation of DOM Node interface
 * Eventually NINode should probably
 * be an interface, but for now write the class NINode
 * and convert to interface later
 */

import org.w3c.dom.*;

import niagara.utils.PEException;

public class NINode {
    private Node domNode;

    /* the tag name of Text elements in dom trees */
    public static final String TEXT = "Text";

    /* DOM Functions */
    public String getNodeName() {
	return domNode.getNodeName();
    }

    public String getNodeValue() {
	return domNode.getNodeValue();
    }

    public void setNodeValue(String nodeValue) {
	domNode.setNodeValue(nodeValue);
    }

    public void replaceChild(NINode oldChild, NINode newChild) {
	/* this must be implemented by NIElement and NIDocument
	 * themselves, replaceChild isn't allowed on attributes
	 */
	throw new PEException("Invalid replaceChild operation");
    }
}
