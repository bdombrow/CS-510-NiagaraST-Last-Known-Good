package niagara.ndom.saxdom;

import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class EntityReferenceImpl extends NodeImpl implements EntityReference {

	public EntityReferenceImpl(DocumentImpl doc, int index) {
		super(doc, index);
	}

	public short getNodeType() {
		return Node.ENTITY_REFERENCE_NODE;
	}

	public String getNodeName() {
		// XXX vpapad: What should we return here?
		return null;
	}

}
