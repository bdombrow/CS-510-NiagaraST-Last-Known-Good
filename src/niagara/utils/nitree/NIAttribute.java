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

public class NIAttribute extends NINode {

    /* reference to dom element associated with this NIElement */
    Attr domAttr;
    boolean initialized;
    NIElement niElt;

    public NIAttribute() {
	domAttr = null;
	initialized = false;
	niElt = null;
    }

    /* FOR CONVERSION FROM DOM */
    public void initialize(Attr _domAttr) {
	domAttr = _domAttr;
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
    public void setValue(String value) {
	/* check for equality of values first to avoid
	 * unnecessary copies - would be ideal if we had
	 * a type here.
	 */
	if(!value.equals(domAttr.getValue())) {
	    niElt.setAttribute(domAttr.getName(), value);
	}
	   return;
    }

    /* 
     * COULD BE ELEMENT-HELPER FUNCTIONS (no state required)
     */
    Attr getDomAttr() {
	return domAttr;
    }

}

