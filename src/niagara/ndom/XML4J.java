/**
 * $Id: XML4J.java,v 1.1 2001/07/17 03:36:37 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.*;
import com.ibm.xml.dom.DocumentImpl; 

/**
 * <code>XML4J</code> is another IBM DOM implementation.
 *
 * @author <a href="mailto:vpapad@cse.ogi.edu">Vassilis Papadimos</a>
 */
class XML4J implements DOMImplementation {

    public Document newDocument() {
        return new DocumentImpl();
    }

    public DOMParser newParser() {
        return new XML4JParser();
    }

    public Node importNode(Document d, Node n) {
        return d.importNode(n, true);
    }
}
