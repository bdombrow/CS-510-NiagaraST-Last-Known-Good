/**
 * $Id: XercesJ2.java,v 1.5 2003/03/07 23:45:30 vpapad Exp $
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
 */
class XercesJ2 implements DOMImplementation {

    DocumentBuilderFactory dbf;
    DocumentBuilderFactory validatingDBF;

    public XercesJ2() {
        dbf = DocumentBuilderFactory.newInstance();

        // turn off validation
        //dbf.setAttribute("http://xml.org/sax/features/validation", new Boolean(false));
        //dbf.setAttribute("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", new Boolean(false));
        //dbf.setAttribute("http://apache.org/xml/features/nonvalidating/load-external-dtd", new Boolean(false));

        // create all nodes right away
        dbf.setAttribute(
            "http://apache.org/xml/features/dom/defer-node-expansion",
            new Boolean(false));

        validatingDBF = DocumentBuilderFactory.newInstance();

        // turn on validation
        validatingDBF.setAttribute(
            "http://xml.org/sax/features/validation",
            new Boolean(true));

        // create all nodes right away
        validatingDBF.setAttribute(
            "http://apache.org/xml/features/dom/defer-node-expansion",
            new Boolean(false));

    }

    public Document newDocument() {
        return new DocumentImpl();
    }

    public DOMParser newParser() {
        return new niagara.ndom.XercesJ2Parser(dbf);
    }

    public DOMParser newValidatingParser() {
        return new niagara.ndom.XercesJ2Parser(validatingDBF);
    }

    public Node importNode(Document d, Node n) {
        try {
            return d.importNode(n, true);
        } catch (DOMException e) { // XXX vpapad
            System.err.println(
                "Caught a DOMException while importing:"
                    + niagara.utils.XMLUtils.flatten(n, true)
                    + " into "
                    + niagara.utils.XMLUtils.flatten(d, true));
            System.out.println("XXX vpapad: doc: " + d.hasChildNodes());
            System.out.println(
                "XXX vpapad: offset: "
                    + niagara.ndom.saxdom.BufferManager.getOffset(
                        ((niagara.ndom.saxdom.NodeImpl) n).getIndex()));
            niagara
                .ndom
                .saxdom
                .BufferManager
                .getPage(((niagara.ndom.saxdom.NodeImpl) n).getIndex())
                .show();

            // What's wrong with the @#$@#$ node
            Node nc = n.getFirstChild();
            while (nc != null) {
                System.err.println(
                    "child: " + nc.getNodeType() + "#" + nc.getNodeValue());
                nc = nc.getNextSibling();
            }
            throw e;
        }
    }

}
