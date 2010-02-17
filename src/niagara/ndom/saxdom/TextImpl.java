package niagara.ndom.saxdom;

import niagara.utils.PEException;

import org.w3c.dom.DOMException;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 * 
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

public class TextImpl extends CharacterDataImpl implements Text {
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

	public String getWholeText() {
		throw new PEException("Not implemented yet");
	}

	public boolean isElementContentWhitespace() {
		throw new PEException("Not implemented yet");
	}

	public Text replaceWholeText(String content) throws DOMException {
		throw new PEException("Not implemented yet");
	}
}
