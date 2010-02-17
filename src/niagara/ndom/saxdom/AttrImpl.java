package niagara.ndom.saxdom;

import niagara.utils.PEException;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;

/**
 * 
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

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

	public Node getFirstChild() {
		return BufferManager.fakeAttributeChildren(doc, index);
	}

	public NodeList getChildNodes() {
		return BufferManager.fakeGetChildNodes(doc, index);
	}

	public TypeInfo getSchemaTypeInfo() {
		throw new PEException("Not implemented yet");
	}

	public boolean isId() {
		throw new PEException("Not implemented yet");
	}
}
