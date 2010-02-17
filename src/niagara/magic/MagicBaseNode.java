package niagara.magic;

import niagara.utils.PEException;
import niagara.utils.Tuple;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.UserDataHandler;

/**
 * <code>MagicRoot</code> DOM element - but has a pointer to tuple...
 */

public abstract class MagicBaseNode implements Node {

	protected MagicBaseNode root;
	private Document doc; // MAJOR HACK !! avoids writing a TextImpl

	protected MagicBaseNode(MagicBaseNode root) {
		this.root = root;
	}

	public void youAreTheRoot(Document doc) {
		this.root = this;
		this.doc = doc;
	}

	protected Document getDoc() {
		return doc;
	}

	public void setTuple(Tuple cTuple) {
		root.setYourNode(cTuple);
	}

	public abstract void setYourNode(Tuple cTuple);

	/*
	 * public StreamTupleElement getCurrentTuple() { return currentTuple; }
	 */

	public void setNextSibling(Node next) throws DOMException {
		assert false : "Calling on wrong class - not supported";
		throw new PEException("Calling on wrong class - not supported");
	}

	public void appendChildDNS(MagicBaseNode newChild) {
		// assert doesn't work here
		assert false : "test0";
		throw new PEException("Calling on wrong class - not supported");
	}

	// NODE interface fcns
	public Document getOwnerDocument() throws DOMException {
		return root.getDoc();
	}

	// NODE INTERFACE METHODS:

	public Node appendChild(Node newChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public abstract Node cloneNode(boolean deep);

	public abstract NamedNodeMap getAttributes();

	public abstract NodeList getChildNodes();

	public abstract Node getFirstChild();

	public abstract Node getLastChild();

	public abstract String getLocalName();

	public abstract String getNamespaceURI();

	public abstract Node getNextSibling();

	public abstract String getNodeName();

	public abstract short getNodeType();

	public abstract String getNodeValue() throws DOMException;

	public Node getParentNode() {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getParentNode unsupported on MagicNode");
	}

	public abstract String getPrefix();

	public abstract Node getPreviousSibling();

	public abstract boolean hasAttributes();

	public abstract boolean hasChildNodes();

	public Node insertBefore(Node newChild, Node refChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public boolean isSupported(String feature, String version) {
		return false; // HACK! (stolen from Vassilis...)
	}

	public abstract void normalize();

	public Node removeChild(Node oldChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setNodeValue(String nodeValue) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setPrefix(String prefix) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public short compareDocumentPosition(Node other) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public String getBaseURI() {
		throw new PEException("Not implemented yet");
	}

	public Object getFeature(String feature, String version) {
		throw new PEException("Not implemented yet");
	}

	public String getTextContent() throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public Object getUserData(String key) {
		throw new PEException("Not implemented yet");
	}

	public boolean isDefaultNamespace(String namespaceURI) {
		throw new PEException("Not implemented yet");
	}

	public boolean isEqualNode(Node arg) {
		throw new PEException("Not implemented yet");
	}

	public boolean isSameNode(Node other) {
		throw new PEException("Not implemented yet");
	}

	public String lookupNamespaceURI(String prefix) {
		throw new PEException("Not implemented yet");
	}

	public String lookupPrefix(String namespaceURI) {
		throw new PEException("Not implemented yet");
	}

	public void setTextContent(String textContent) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public Object setUserData(String key, Object data, UserDataHandler handler) {
		throw new PEException("Not implemented yet");
	}
}
