/**
 * $Id: NodeListImpl.java,v 1.2 2002/04/30 21:26:07 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;
import java.util.ArrayList;

public class NodeListImpl implements NodeList {

    private ArrayList items;

    public NodeListImpl(ArrayList items) {
        this.items = items;
    }

    public Node item(int index) {
        if (index < 0 || index >= items.size())
            return null;
        else return (Node) items.get(index);
    }

    public int getLength() {
        return items.size();
    }

}
