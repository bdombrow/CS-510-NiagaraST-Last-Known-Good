/**
 * $Id: XercesJ.java,v 1.2 2002/03/26 23:52:07 tufte Exp $
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

    public Node importNode(Document d, Node n) {
        return d.importNode(n, true);
    }

}
