package niagara.ndom.saxdom;

import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class DocumentFragmentImpl extends NodeImpl implements DocumentFragment {
	// private Document doc;

	public DocumentFragmentImpl(DocumentImpl doc) {
		// XXX vpapad: I don't like this -1 business, but...
		super(doc, -1);
	}

	public short getNodeType() {
		return Node.DOCUMENT_FRAGMENT_NODE;
	}

	public String getNodeName() {
		return "#document-fragment";
	}
}
