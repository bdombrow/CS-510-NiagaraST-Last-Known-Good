/**
 * $Id: DOMFactory.java,v 1.1 2001/07/17 03:36:36 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.*;
import org.xml.sax.*;

import java.util.Hashtable;

/**
 * <code>DOMFactory</code> allows us to choose
 * a DOM implementation at runtime.
 *
 * @author <a href="mailto:vpapad@cse.ogi.edu">Vassilis Papadimos</a>
 */
public class DOMFactory {

    /**
     * <code>impl</code> is the default DOM implementation used for new parsers
     * and documents.
     *
     */
    private static DOMImplementation impl;

    private static Hashtable name2impl;
    static {
        name2impl = new Hashtable();
        name2impl.put("xml4j", new XML4J());
        name2impl.put("xerces", new XercesJ()); 

        // xerces is the default
        impl = (DOMImplementation) name2impl.get("xerces");
    }

    /**
     * <code>setImpl</code> sets the default DOM implementation.
     *
     * @param name a <code>String</code> value
     */
    public static void setImpl(String name) {
        if (name2impl.get(name) == null)
            System.err.println("DOM Implementation " + name + " is not registered!");
        else
            impl = (DOMImplementation) name2impl.get(name);
    }

    /**
     * <code>newDocument</code> creates a new Document with the 
     * default implementation.
     *
     * @return a <code>Document</code> value
     */
    public static Document newDocument() {
        return impl.newDocument();
    }


    /**
     * <code>newParser</code> creates a new Parser with the
     * default implementation.
     *
     * @return a <code>DOMParser</code> value
     */
    public static DOMParser newParser() {
        return impl.newParser();
    }


    /**
     * Returns a version of a node that can be inserted in 
     * a (possibly different than its current) document 
     * (by deep cloning it if needed)
     *
     * This method is here just for the benefit of the old TXDOM parser
     * -- it is part of DOM Level 2
     *
     * @param d a <code>Document</code> 
     * @param n a <code>Node</code> 
     * @return the (possibly cloned) <code>Node</code>
     */
    public static Node importNode(Document d, Node n) {
        if (n.getOwnerDocument() == d)
            return n;
        else 
            return impl.importNode(d, n);
    }
}




