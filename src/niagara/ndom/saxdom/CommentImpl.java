package niagara.ndom.saxdom;

import org.w3c.dom.Node;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class CommentImpl extends CharacterDataImpl {

	public CommentImpl(DocumentImpl doc, int index) {
		super(doc, index);
	}

	public short getNodeType() {
		return Node.COMMENT_NODE;
	}

	public String getNodeName() {
		// XXX vpapad: What are we supposed to return here?
		return null;
	}
}
