/**
 * $Id: AttrImpl.java,v 1.3 2002/04/17 03:09:18 vpapad Exp $
 *
 * A read-only implementation of the DOM Level 2 interface,
 * using an array of SAX events as the underlying data store.
 *
 */

package niagara.ndom.saxdom;

import org.w3c.dom.*;

public class AttrImpl extends NodeImpl implements Attr {

    // XXX vpapad: Comments? COMMENTS?! We don't need no stinkin' comments!
    // See the documentation for DOM2 core interfaces.

    public AttrImpl(DocumentImpl doc, int index) {
        super(doc, index);
    }

    public short getNodeType() {
        return Node.ATTRIBUTE_NODE;
    }

    public String getNodeName() {
        return BufferManager.getName(index);
    }

    public String getNodeValue() {
	return BufferManager.getAttributeValue(index);
    }

    public String getName() {
        return BufferManager.getName(index);
    }

    public boolean getSpecified() {
        // We do not yet support unspecified attributes (implied or
        // defaulted through the DTD). This can be implemented using
        // getSpecified() from SAX2's Attributes2 interface.

        return true;
    }

    public String getValue() {
        return BufferManager.getAttributeValue(index);
    }

    public void setValue(String value) throws DOMException {
        throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
                               "SAXDOM objects are read-only.");
    }

    public Element getOwnerElement() {
        return BufferManager.getOwnerElement(index);
    }

}
