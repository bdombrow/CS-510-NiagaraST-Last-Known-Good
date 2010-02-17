package niagara.magic;

import niagara.utils.Tuple;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * MagicContentAttr - holds place of XML attribute where tag name is specified
 * but content comes from another attribute in the tuple
 */

public class MagicContentAttr extends MagicNode {

	public MagicContentAttr(int attrIdx, MagicBaseNode root, String attrName) {
		super(attrIdx, root, Node.ATTRIBUTE_NODE);
		this.attrName = attrName;
		textChild = root.getDoc().createTextNode("");
	}

	private Text textChild;
	private String nodeValue;
	private String attrName;
	private Attr myNode;
	private static StringBuffer attrValue = new StringBuffer();
	private boolean hasTextChild = false;

	public void setYourNode(Tuple cTuple) {
		myNode = (Attr) cTuple.getAttribute(attrIdx);
		nodeValue = null;

		hasTextChild = false;
		String value = getNodeValue();
		if (value != null) {
			hasTextChild = true;
			textChild.setData(value);
		}
	}

	// NODE INTERFACE METHODS:
	public Node appendChild(Node newChild) throws DOMException {
		assert false : "attributes can't have children";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have children.");
	}

	public Node cloneNode(boolean deep) {
		System.out.println("KT: HEY cloning magic ATTR");
		if (myNode != null) {
			Attr a = root.getDoc().createAttribute(attrName);
			a.setValue(getNodeValue());
			return a;
		} else {
			System.out.println("KT WARNING MAGIC CLONE");
			return new MagicContentAttr(attrIdx, root, attrName);
		}
	}

	public NamedNodeMap getAttributes() throws DOMException {
		assert false : "attributes can't have attributes";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have attributes.");
	}

	public NodeList getChildNodes() throws DOMException {
		assert false : "attributes can't have children";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have children.");
	}

	public Node getFirstChild() throws DOMException {
		if (hasTextChild)
			return textChild;
		else
			return null;
	}

	public Node getLastChild() throws DOMException {
		return getFirstChild();
	}

	public String getLocalName() {
		return attrName;
	}

	public String getNamespaceURI() {
		return myNode.getNamespaceURI();
	}

	public Node getNextSibling() throws DOMException {
		assert false : "attributes can't have siblings";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have siblings.");
	}

	public String getNodeName() throws DOMException {
		return attrName;
	}

	public short getNodeType() {
		return org.w3c.dom.Node.ATTRIBUTE_NODE;
	}

	public String getNodeValue() throws DOMException {
		if (nodeValue != null)
			return nodeValue;

		// below stolen from addAttributes function
		Node attr = myNode;
		if (attr instanceof Element) {
			assert false : "Should not get here!";
			Element elt = (Element) attr;

			// Concatenate the node values of
			// the element's children
			attrValue.setLength(0);
			Node n = elt.getFirstChild();
			while (n != null) {
				attrValue.append(n.getNodeValue());
				n = n.getNextSibling();
			}
			nodeValue = attrValue.toString();
		} else if (attr instanceof Attr) {
			nodeValue = ((Attr) attr).getValue();
		} else {
			assert false : "KT: what did I get here?? "
					+ attr.getClass().getName();
		}
		return nodeValue;
	}

	public Node getParentNode() throws DOMException {
		assert false : "getParentNode unsupported";
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getParentNode unsupported on MagicContent");
	}

	public String getPrefix() throws DOMException {
		assert false : "getPrefix unsupported";
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getPrefix unsupported on MagicContent");
	}

	public Node getPreviousSibling() throws DOMException {
		assert false : "attributes can't have siblings";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have siblings.");
	}

	public boolean hasAttributes() throws DOMException {
		assert false : "attributes can't have attributes";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have attributes.");
	}

	public boolean hasChildNodes() throws DOMException {
		assert false : "attributes can't have children";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have children.");
	}

	public Node insertBefore(Node newChild, Node refChild) throws DOMException {
		assert false : "attributes can't have children";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have children.");
	}

	public boolean isSupported(String feature, String version) {
		// copy vassilis - reasonable answer...
		return false;
	}

	public void normalize() throws DOMException {
		assert false : "normalize not supported";
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Normalize not supported");
	}

	public Node removeChild(Node oldChild) throws DOMException {
		assert false : "attributes can't have children";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have children.");
	}

	public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
		assert false : "attributes can't have children";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"Attributes can't have children.");
	}

	public void setNodeValue(String nodeValue) throws DOMException {
		assert false : "magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setPrefix(String prefix) throws DOMException {
		assert false : "magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	// ATTR INTERFACE METHODS
	public String getName() {
		return attrName;
	}

	public Element getOwnerElement() throws DOMException {
		assert false : "getownerelement not supported";
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Get owner element not supported on Magic Nodes");
	}

	public boolean getSpecified() throws DOMException {
		assert false : "getspecify not supported";
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"getSpecify not supported on Magic Content");
	}

	public String getValue() {
		return getNodeValue();
	}

	public void setValue(String value) throws DOMException {
		assert false : "magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	// KT - this is ugly and embarassing. MagicContentAttr inherits
	// functions for the Element, Text and CharacterData interfaces
	// from MagicNode which implements them all. MagicNode has to
	// implement them all since we don't know if we are creating
	// a MagicElement/Attr/Text when creating a MagicNode. However,
	// we want to separate out the Attr and Element MagicContent classes
	// for clarity of coding !*!@?#!#$

	// ELEMENT INTERFACE METHODS
	public String getAttribute(String name) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public Attr getAttributeNode(String name) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public Attr getAttributeNodeNS(String namespaceURI, String localName)
			throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public String getAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public NodeList getElementsByTagName(String name) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public NodeList getElementsByTagNameNS(String namespaceURI, String localName)
			throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public String getTagName() throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public boolean hasAttribute(String name) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public boolean hasAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public void removeAttribute(String name) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public void removeAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public void setAttribute(String name, String value) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public Attr setAttributeNode(Attr newAttr) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	public void setAttributeNS(String namespaceURI, String qualifiedName,
			String value) throws DOMException {
		assert false : "magic content attr does not support element interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support element interface");
	}

	// TEXT INTERFACE METHODS
	public Text splitText(int offset) throws DOMException {
		assert false : "magic content attr does not support text interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support text interface");
	}

	// CHARACTER DATA INTERFACE METHODS
	public void appendData(String arg) throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public void deleteData(int offset, int count) throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public String getData() throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public int getLength() throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public void insertData(int offset, String arg) throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public void replaceData(int offset, int count, String arg)
			throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public void setData(String data) throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

	public String substringData(int offset, int count) throws DOMException {
		assert false : "magic content attr does not support char data interface";
		throw new DOMException(DOMException.INVALID_ACCESS_ERR,
				"MagicContentAttr does not support character data interface");
	}

}
