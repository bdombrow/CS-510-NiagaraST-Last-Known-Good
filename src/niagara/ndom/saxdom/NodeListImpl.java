/**
 * $Id: NodeListImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class NodeListImpl implements NodeList {

    private Node[] items;

    public NodeListImpl(Node[] items) {
        this.items = items;
    }

    public Node item(int index) {
        if (index < 0 || index >= items.length)
            return null;
        else return items[index];
    }

    public int getLength() {
        return items.length;
    }

}
