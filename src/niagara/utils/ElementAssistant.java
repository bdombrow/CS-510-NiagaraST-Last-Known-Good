package niagara.utils;

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */


/**
 * A class to add some functionality to DOM elements. In particular,
 * it is often necessary to retrieve only element children of
 * a node. DOM only does element decendents, so we add assistor
 * functions to take care of this
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

import java.util.*;
import java.io.*;
import org.w3c.dom.*;

public class ElementAssistant {

    /* this class should never be instantiated */
    private void ElementAssistant() {}

    /**
     * Returns the first element child of the given node
     *
     * @param node The element to return the child of
     *
     * @return The first element child of node
     */
    public static Element getFirstElementChild(Node node) {
	Node child = node.getFirstChild();
	while(child != null && child.getNodeType() != Node.ELEMENT_NODE) {
	    child = child.getNextSibling();
	}
	/* hmm, I wonder if this will work if child is null... */
	return (Element) child;
    }

    public static Element getNextElementSibling(Node node) {
	Node child = node.getNextSibling();
	while(child != null && child.getNodeType() != Node.ELEMENT_NODE) {
	    child = child.getNextSibling();
	}
	/* hmm, I wonder if this will work if child is null... */
	return (Element) child;
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

