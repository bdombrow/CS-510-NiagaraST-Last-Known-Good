/**
 * $Id: NamedNodeMapImpl.java,v 1.5 2003/07/03 19:40:39 tufte Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;
import org.xml.sax.Attributes;

public class NamedNodeMapImpl implements NamedNodeMap {

    private DocumentImpl doc;
    private int index;

    /**
     * The only way anyone is ever going to get a NamedNodeMap
     * out of SAXDOM is by calling Element.getAttributes()
     * We use this fact to implement this class without any
     * local storage.
     */
    public NamedNodeMapImpl(DocumentImpl doc, int index) {
        this.doc = doc;
	this.index = index;
    }

    public Node getNamedItem(String name) {
	return BufferManager.getAttributeNode(doc, index, name);
    }

    public Node setNamedItem(Node arg) throws DOMException {
	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
			       "SAXDOM objects are read-only");
    }

    public Node removeNamedItem(String name) throws DOMException {
	throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
			       "SAXDOM objects are read-only");
    }

    public Node item(int attrIndex) {
	return BufferManager.getAttributeByIndex(doc, index, attrIndex);
    }

    public int getLength() {
        return BufferManager.getNumberOfAttributes(doc, index);
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

