package niagara.ndom.saxdom;

import niagara.utils.PEException;

import org.w3c.dom.Entity;
import org.w3c.dom.Node;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class EntityImpl extends NodeImpl implements Entity {

	public EntityImpl(DocumentImpl doc, int index) {
		super(doc, index);
	}

	public short getNodeType() {
		return Node.ENTITY_NODE;
	}

	public String getNodeName() {
		// XXX vpapad: What should we return here?
		return null;
	}

	public String getPublicId() {
		return BufferManager.getPublicId(index);
	}

	public String getSystemId() {
		return BufferManager.getSystemId(index);
	}

	public String getNotationName() {
		return BufferManager.getNotationName(index);
	}

	public String getInputEncoding() {
		throw new PEException("Not implemented yet");
	}

	public String getXmlEncoding() {
		throw new PEException("Not implemented yet");
	}

	public String getXmlVersion() {
		throw new PEException("Not implemented yet");
	}
}
