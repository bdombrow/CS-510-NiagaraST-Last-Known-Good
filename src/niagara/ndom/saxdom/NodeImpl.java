/**
 * $Id: NodeImpl.java,v 1.3 2002/04/06 02:15:08 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public abstract class NodeImpl implements Node {

    protected DocumentImpl doc;
    protected int index;

    public NodeImpl(DocumentImpl doc, int index) {
        this.doc = doc;
        this.index = index;
    }

    public boolean equals(Object other) {
        if (! (other instanceof NodeImpl))
            return false;
        NodeImpl no = (NodeImpl) other;
        return (no.doc == doc && no.index == index);
    }

    public int hashCode() {
        if (this instanceof DocumentImpl) 
            return super.hashCode();

        return doc.hashCode() ^ index;
    }
    public abstract String getNodeName();

    public String getNodeValue() throws DOMException {
        // Default implementation - overriden in some subclasses
        return null;
    }

    public int getIndex() { 
        return index;
    }

    public void setNodeValue(String nodeValue) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public abstract short getNodeType();

    public Node getParentNode() {
        return BufferManager.getParentNode(index);
    }

    public NodeList getChildNodes() {
        return BufferManager.getChildNodes(doc, index);
    }

    public Node getFirstChild() {
        return BufferManager.getFirstChild(doc, index);
    }

    public Node getLastChild() {
        return BufferManager.getLastChild(doc, index);
    }

    public int getLastChildIndex() {
        return BufferManager.getLastChildIndex(index);
    }

    public Node getPreviousSibling() {
        return BufferManager.getPreviousSibling(doc, index);
    }

    public int getPreviousSiblingIndex() {
        return BufferManager.getPreviousSiblingIndex(index);
    }

    public Node getNextSibling() {
        return BufferManager.getNextSibling(doc, index);
    }

    public NamedNodeMap getAttributes() {
        return BufferManager.getAttributes(doc, index);
    }

    public Document getOwnerDocument() {
        return doc;
    }

    public Node insertBefore(Node newChild, Node refChild)
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public Node replaceChild(Node newChild, Node oldChild) 
        throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public Node removeChild(Node oldChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }


    public Node appendChild(Node newChild) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public boolean hasChildNodes() {
        return BufferManager.hasChildNodes(index);
    }

    public Node cloneNode(boolean deep) {
        return null; // We can't do this, and the spec does not allow us
                     // to throw an exception. Oh well.
    }

    public void normalize() {
       // Do nothing, the SAXDOM parser code should have taken care of this.
    }

    public boolean isSupported(String feature, String version) {
        // XXX vpapad: This is a prudent choice...
        return false;
    }

    public String getNamespaceURI() {
        return BufferManager.getNamespaceURI(index);
    }

    public String getPrefix() {
        return BufferManager.getPrefix(index);
    }

    public void setPrefix(String prefix) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public String getLocalName() {
        return BufferManager.getLocalName(index);
    }

    public boolean hasAttributes() {
        return BufferManager.hasAttributes(index);
    }

    // XXX vpapad:
    // The following methods are from DOM Level 3, but we either have
    // to "implement" them, or go through major hoops to compile two
    // different DOM interface definitions (the standard one, and the
    // one included in Xerces) in the same project...

    public Node adoptNode(Node n) throws org.w3c.dom.DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public String getEncoding() {
        return null;
    }

    public void setEncoding(String s) {}

    public boolean getStandalone() {
        return true;
    }

    public void setStandalone(boolean b) {}

    public boolean getStrictErrorChecking() {
        return false;
    }
    public void setStrictErrorChecking(boolean b) {}

    public String getVersion() {
        return null;
    }
    public void setVersion(String s) {}
}
