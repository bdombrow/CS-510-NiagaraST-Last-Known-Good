/**
 * $Id: TextImpl.java,v 1.3 2002/04/06 02:15:08 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class TextImpl  extends CharacterDataImpl implements Text {
    public TextImpl(DocumentImpl doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.TEXT_NODE;
    }

    public String getNodeName() {
        return "#text";
    }

    public Text splitText(int offset) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }


}
