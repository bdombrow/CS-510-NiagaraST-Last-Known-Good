/**
 * $Id: DOMImplementation.java,v 1.2 2002/10/31 04:29:27 vpapad Exp $
 *
 */

package niagara.ndom;

import org.w3c.dom.*;

/**
 * <code>DOMImplementation</code> is the interface implemented
 * by all DOM implementations.
 */
public interface DOMImplementation {

    /**
     * <code>newDocument</code> creates a new document
     * node for this particular DOM implementation
     *
     * @return the new <code>Document</code> node
     */
    Document newDocument();


    /**
     * <code>newParser</code> creates a new parser
     * for this particular DOM implementation
     *
     * @return the new <code>DOMParser</code> 
     */
    DOMParser newParser();

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
    Node importNode(Document d, Node n);
}
