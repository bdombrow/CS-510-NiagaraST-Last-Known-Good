/**
 * $Id: ElementImpl.java,v 1.1 2002/03/26 22:07:50 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class ElementImpl extends NodeImpl implements Element {

    // XXX vpapad: Comments? COMMENTS?! We don't need no stinkin' comments!
    // See the documentation for DOM2 core interfaces.

    public ElementImpl(Document doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.ELEMENT_NODE;
    }

    public String getNodeName() {
        return BufferManager.getTagName(index);
    }

    public String getTagName() {
        return BufferManager.getTagName(index);
    }

    public String getAttribute(String name) {
        return BufferManager.getAttribute(index, name).getValue();
    }

    public void setAttribute(String name, String value)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public void removeAttribute(String name)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public Attr getAttributeNode(String name) {
        return BufferManager.getAttributeNode(index, name);
    }

    public Attr setAttributeNode(Attr newAttr)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public Attr removeAttributeNode(Attr oldAttr)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public NodeList getElementsByTagName(String name) {
        return BufferManager.getElementsByTagName(index, name);
    }

    public String getAttributeNS(String namespaceURI, String localName) {
        return BufferManager.getAttributeNS(index, namespaceURI, localName)
            .getValue();
    }

    public void setAttributeNS(String namespaceURI, String qualifiedName, 
                               String value)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public void removeAttributeNS(String namespaceURI, String localName)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public Attr getAttributeNodeNS(String namespaceURI, String localName) {
        return BufferManager.getAttributeNodeNS(index, 
                                                namespaceURI, localName);
    }

    public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public NodeList getElementsByTagNameNS(String namespaceURI, 
                                           String localName) {
        return BufferManager.getElementsByTagNameNS(index, namespaceURI,
                                                    localName);
    }

    public boolean hasAttribute(String name) {
        return BufferManager.hasAttribute(index, name);
    }

    public boolean hasAttributeNS(String namespaceURI, String localName) {
        return BufferManager.hasAttributeNS(index, namespaceURI, localName);
    }

}
