/**
 * $Id: TextImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class TextImpl  extends CharacterDataImpl implements Text {
    public TextImpl(Document doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.TEXT_NODE;
    }

    public String getNodeName() {
        // XXX vpapad: What are we supposed to return here?
        return null;
    }

    public Text splitText(int offset) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }


}
