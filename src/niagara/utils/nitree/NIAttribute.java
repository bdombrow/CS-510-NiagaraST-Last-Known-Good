package niagara.utils.nitree;

/* Class that extends DOM attribute interface and participates
 * in copy on write semantics of the tree
 *
 * GOOD QUESTION: can I get by without writing this class?
 * Options 1) Don't write this class - unsafe and fast
 *   2) Write this class - safe and slow
 *  If I write this class, disallow set functions through
 *  the attribute - or rather route those functions through the
 *  associated Element - don't want to do mapping and copy on
 *  write on attributes - want that to be limited to Elements
 * Pick option 2
 */

import org.w3c.dom.*;
import niagara.utils.*;

public class NIAttribute extends NINode {

    /* reference to dom element associated with this NIElement */
    Attr domAttr;
    boolean initialized;
    NIElement parentElt;

    public NIAttribute() {
	domAttr = null;
	initialized = false;
	parentElt = null;
    }

    /* FOR CONVERSION FROM DOM */
    public void initialize(Attr _domAttr, NIElement _parentElt) {
	domAttr = _domAttr;
	parentElt = _parentElt;
	initialized = true;
    }

    public boolean isInitialized() { return initialized; }


    /* 
     * DOM defined functions 
     */
    /** returns name of attr - defined by DOM 
     */
    public String getName() {
	return domAttr.getName();
    }

    /** returns if attr specified in input or not - defined by DOM
     */
    public boolean getSpecified() {
	return domAttr.getSpecified(); 
    }

    /** returns value of attr - defined by dOM
     */
    public String getValue() {
	return domAttr.getValue();
    }
    
    /** sets value of attr - defined by DOM
     */
    public void setValue(String value) 
	throws NITreeException {
	/* check for equality of values first to avoid
	 * unnecessary copies - would be ideal if we had
	 * a type here.
	 */
	if(!value.equals(domAttr.getValue())) {
	    parentElt.setAttribute(domAttr.getName(), value);
	}
	   return;
    }

    public String getNodeName() {
	return domAttr.getNodeName();
    }

    public String getNodeValue() {
	return domAttr.getNodeValue();
    }

    public void setNodeValue(String nodeValue) 
	throws NITreeException {
	setValue(nodeValue);
    }

    public void replaceChild(NIElement newChild, NIElement oldChild) 
	throws NITreeException {
	throw new PEException("Replace child not allowed on NIAttribute");
    }

    public void appendChild(NIElement child) 
	throws NITreeException {
	throw new PEException("Append child not allowed on NIAttribute");
    }

    /* 
     * COULD BE ELEMENT-HELPER FUNCTIONS (no state required)
     */
    Attr getDomAttr() {
	return domAttr;
    }

    public String myGetNodeValue() {
	return getNodeValue();
    }

    public void mySetNodeValue(String nodeValue) 
	throws NITreeException{
	setNodeValue(nodeValue);
    }

}

