/**
 * $Id: XercesJ.java,v 1.4 2003/07/09 04:59:37 tufte Exp $
 *
 */

package niagara.ndom;

import org.apache.xerces.dom.*;
import org.w3c.dom.*;

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
