package niagara.ndom;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.ibm.xml.dom.DocumentImpl;

/**
 * <code>XML4J</code> is another IBM DOM implementation.
 */
class XML4J implements DOMImplementation {

	public Document newDocument() {
		return new DocumentImpl();
	}

	public DOMParser newParser() {
		return new XML4JParser();
	}

	public DOMParser newValidatingParser() {
		// XXX vpapad: how do you make an XML4J parser validating?
		// Do we even care?
		return new XML4JParser();
	}

	public Node importNode(Document d, Node n) {
		return d.importNode(n, true);
	}

	public boolean supportsStreaming() {
		return false;
	}
}
