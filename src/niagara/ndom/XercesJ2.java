/**
 * $Id: XercesJ2.java,v 1.1 2002/03/26 23:52:07 tufte Exp $
 *
 */

package niagara.ndom;

import org.apache.xerces.dom.*;
import org.w3c.dom.*;
import niagara.utils.XMLUtils;
import javax.xml.parsers.*;

/**
 * <code>XercesJ</code> wraps the Apache Xerces DOM implementation.
 *
 * @author <a href="mailto:vpapad@cse.ogi.edu">Vassilis Papadimos</a>
 */
class XercesJ2 implements DOMImplementation {

    DocumentBuilderFactory dbf;

    public XercesJ2() {

	dbf = DocumentBuilderFactory.newInstance();
	
	// turn off validation
	//dbf.setAttribute("http://xml.org/sax/features/validation", new Boolean(false));
	//dbf.setAttribute("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", new Boolean(false));
	//dbf.setAttribute("http://apache.org/xml/features/nonvalidating/load-external-dtd", new Boolean(false));
	
	// create all nodes right away
	dbf.setAttribute("http://apache.org/xml/features/dom/defer-node-expansion", new Boolean(false));
	
    }

    public Document newDocument() {
        return new DocumentImpl(); 
    }

    public DOMParser newParser() {
        return new niagara.ndom.XercesJ2Parser(dbf);
    }

    public Node importNode(Document d, Node n) {
        return d.importNode(n, true);
    }

}




