package niagara.magic;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * MagicContentElement - holds the place of magic element content - this is an
 * element with specified tag name and attribute and content comes from another
 * attribute in the tuple (attrIdx)
 */

public class MagicContentElement extends MagicNode {

	public MagicContentElement(int attrIdx, MagicBaseNode root, String tagName) {
		super(attrIdx, root, Node.ELEMENT_NODE);
		this.tagName = tagName;
		attrList = null;
	}

	private String tagName;
	private NamedNodeMapImpl attrList;
	private Element myNode;

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	// NODE INTERFACE METHODS:
	public Node appendChild(Node newChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public Node cloneNode(boolean deep) throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Can not clone MagicElementContent.");
	}

	public NamedNodeMap getAttributes() {
		return attrList;
	}

	public NodeList getChildNodes() throws DOMException {
		return myNode.getChildNodes();
	}

	public Node getFirstChild() throws DOMException {
		return myNode.getFirstChild();
	}

	public Node getLastChild() {
		return myNode.getLastChild();
	}

	public String getLocalName() {
		return tagName;
	}

	public Node getNextSibling() throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getNextSibling unsupported on MagicContent");
	}

	public String getNodeName() throws DOMException {
		return tagName;
	}

	public short getNodeType() throws DOMException {
		return Node.ELEMENT_NODE;
	}

	public String getNodeValue() throws DOMException {
		return null; // correct for element??
	}

	public Node getParentNode() throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getParentNode unsupported on MagicContent");
	}

	public String getPrefix() throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getPrefix unsupported on MagicContent");
	}

	public Node getPreviousSibling() {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getPreviousSibling unsupported on MagicContent");
	}

	public boolean hasAttributes() {
		if (attrList == null)
			return false;
		else
			return true;
	}

	public boolean hasChildNodes() {
		return myNode.hasChildNodes();
	}

	public Node insertBefore(Node newChild, Node refChild) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public boolean isSupported(String feature, String version) {
		return myNode.isSupported(feature, version);
	}

	public void normalize() {
		myNode.normalize();
	}

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

	// ELEMENT INTERFACE METHODS
	public String getAttribute(String name) {
		return (attrList.getNamedItem(name)).getNodeValue();
	}

	public Attr getAttributeNode(String name) {
		return (Attr) attrList.getNamedItem(name);
	}

	public Attr getAttributeNodeNS(String namespaceURI, String localName) {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Namespaces not supported in Magic.");
	}

	public String getAttributeNS(String namespaceURI, String localName) {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Namespaces not supported in Magic.");
	}

	public NodeList getElementsByTagName(String name) {
		// KT - can we get something other than an element here???
		return ((Element) myNode).getElementsByTagName(name);
	}

	public NodeList getElementsByTagNameNS(String namespaceURI, String localName) {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Namespaces not supported in Magic.");
	}

	public String getTagName() {
		return tagName;
	}

	public boolean hasAttribute(String name) {
		if (attrList.getNamedItem(name) != null)
			return true;
		else
			return false;
	}

	public boolean hasAttributeNS(String namespaceURI, String localName) {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Namespaces not supported in Magic.");
	}

	public void removeAttribute(String name) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void removeAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setAttribute(String name, String value) throws DOMException {
		if (attrList == null)
			attrList = new NamedNodeMapImpl();
		Attr a = ((Node) root).getOwnerDocument().createAttribute(name);
		a.setValue(value);
		attrList.setNamedItem(a);
	}

	public Attr setAttributeNode(Attr newAttr) throws DOMException {
		if (attrList == null)
			attrList = new NamedNodeMapImpl();
		return (Attr) attrList.setNamedItem(newAttr);
	}

	public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Namespaces not supported in Magic.");
	}

	public void setAttributeNS(String namespaceURI, String qualifiedName,
			String value) throws DOMException {
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Namespaces not supported in Magic.");
	}

	// KT - this is ugly and embarassing. MagicContentAttr inherits
	// functions for the Element, Text and CharacterData interfaces
	// from MagicNode which implements them all. MagicNode has to
	// implement them all since we don't know if we are creating
	// a MagicElement/Attr/Text when creating a MagicNode. However,
	// we want to separate out the Attr and Element MagicContent classes
	// for clarity of coding !*!@?#!#$

	// ATTR INTERFACE METHODS
	public String getName() throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support attr interface");
	}

	public Element getOwnerElement() throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support attr interface");
	}

	public boolean getSpecified() {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support attr interface");
	}

	public String getValue() throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support attr interface");
	}

	public void setValue(String value) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support attr interface");
	}

	// TEXT INTERFACE METHODS
	public Text splitText(int offset) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support text interface");
	}

	// CHARACTER DATA INTERFACE METHODS
	public void appendData(String arg) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public void deleteData(int offset, int count) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public String getData() throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public int getLength() throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public void insertData(int offset, String arg) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public void replaceData(int offset, int count, String arg)
			throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public void setData(String data) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}

	public String substringData(int offset, int count) throws DOMException {
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicElementContent does not support char data interface");
	}
}
