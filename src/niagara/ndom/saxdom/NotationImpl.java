/**
 * $Id: NotationImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class NotationImpl extends NodeImpl implements Notation {

    public NotationImpl(Document doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.NOTATION_NODE;
    }

    public String getNodeName() {
        // XXX vpapad: What are we supposed to return here?
        return null;
    }

    public String getPublicId() {
        return BufferManager.getPublicId(index);
    }

    public String getSystemId() {
        return BufferManager.getSystemId(index);
    }

}
