/**
 * $Id: SAXDOM.java,v 1.3 2003/03/07 23:45:30 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.*;
import niagara.utils.XMLUtils;
import niagara.utils.PEException;


/**
 * <code>SAXDOM</code> wraps the read-only "DOM as SAX events" implementation.
 */
class SAXDOM implements DOMImplementation {

    public Document newDocument() {
        throw new PEException("Can't create arbitrary documents with SAXDOM.");
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
	throw new PEException("SAXDOM objects are read-only");
    }
}
