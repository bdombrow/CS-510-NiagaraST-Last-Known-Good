/**
 * $Id: XercesJ.java,v 1.3 2003/03/07 23:45:30 vpapad Exp $
 *
 */

package niagara.ndom;

import org.apache.xerces.dom.*;
import org.w3c.dom.*;
import niagara.utils.XMLUtils;

/**
 * <code>XercesJ</code> wraps the Apache Xerces DOM implementation.
 *
 * @author <a href="mailto:vpapad@cse.ogi.edu">Vassilis Papadimos</a>
 */
class XercesJ implements DOMImplementation {

    public Document newDocument() {
        return new DocumentImpl();
    }

    public DOMParser newParser() {
        return new niagara.ndom.XercesJParser();
    }

    public DOMParser newValidatingParser() {
        // XXX vpapad: how do you make a Xerces1 parser validating?
        // Do we even care?
        return new niagara.ndom.XercesJParser();
    }

    public Node importNode(Document d, Node n) {
        return d.importNode(n, true);
    }
}
