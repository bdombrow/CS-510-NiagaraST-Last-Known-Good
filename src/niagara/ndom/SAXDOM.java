/**
 * $Id: SAXDOM.java,v 1.5 2003/07/09 04:59:37 tufte Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.*;


/**
 * <code>SAXDOM</code> wraps the read-only "DOM as SAX events" implementation.
 */
class SAXDOM implements DOMImplementation {

    public Document newDocument() {
        assert false : "Can't create arbitrary documents with SAXDOM.";
	return null;
    }

    public DOMParser newParser() {
        return new SAXDOMParser();
    }

    public DOMParser newValidatingParser() {
            // XXX vpapad: I don't think it's possible to validate
            // using saxdom...
        return new SAXDOMParser();
    }
    
    public Node importNode(Document d, Node n) {
        // SAXDOM objects are read-only.
	assert false : "SAXDOM objects are read-only";
	return null;
    }
}
