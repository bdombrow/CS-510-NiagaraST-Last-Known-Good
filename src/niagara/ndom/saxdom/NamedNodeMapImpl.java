/**
 * $Id: NamedNodeMapImpl.java,v 1.2 2002/03/27 10:12:10 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;
import java.util.Hashtable;
import java.util.Vector;

public class NamedNodeMapImpl implements NamedNodeMap {

    private Document doc;

    private Hashtable name2node;
    private Vector id2name;

    public NamedNodeMapImpl(DocumentImpl doc) {
        this.doc = doc;
        name2node = new Hashtable();
        id2name = new Vector();
    }

    public Node getNamedItem(String name) {
        return (Node) name2node.get(name);
    }

    public Node setNamedItem(Node arg) throws DOMException {
        if (arg.getOwnerDocument() != doc) 
            throw new DOMException(DOMException.WRONG_DOCUMENT_ERR,
                                    "Node belongs to a different document.");

        // We never throw INUSE_ATTRIBUTE_ERR, attributes are always
        // created as children of exactly one element

        String name = arg.getNodeName();
        Node old = (Node) name2node.get(name);
                
        name2node.put(name, arg);

        for (int i=0; i < id2name.size(); i++) {
            String s = (String) id2name.get(i);

            if (s.equals(name)) { 
                // Replacing
                id2name.set(i, arg);
                return old;
            }
        }
        
        // Appending
        id2name.add(name);
        return old;
    }

    public Node removeNamedItem(String name) throws DOMException {
        Node old = (Node) name2node.get(name);
        if ( old == null) 
            throw new DOMException(DOMException.NOT_FOUND_ERR,
                                   "Node not found in map.");
        
        name2node.remove(name);
        return old;
    }

    public Node item(int index) {
        if (index < 0 || index >= id2name.size())
            return null;
        return (Node) name2node.get(id2name.get(index));
    }

    public int getLength() {
        return id2name.size();
    }

    public Node getNamedItemNS(String namespaceURI, String localName) {
        // XXX vpapad: how do we implement this? How does this interact
        // XXX with plain getNamedItem() ?
        return null;
    }

    // The specification does not allow a NOT_SUPPORTED exception here
    // but we throw one anyway.
    public Node setNamedItemNS(Node arg) throws DOMException {
        // XXX vpapad: how do we implement this?
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "setNamedItemNS not supported.");
    }

    public Node removeNamedItemNS(String namespaceURI, String localName)
        throws DOMException {
        // XXX vpapad: how do we implement this?
        throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
                               "removeNamedItemNS not supported.");
    }

}
