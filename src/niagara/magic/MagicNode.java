package niagara.magic;

import niagara.utils.PEException;
import niagara.utils.Tuple;

import org.w3c.dom.Attr;
import org.w3c.dom.CharacterData;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.w3c.dom.TypeInfo;

/**
 * <code>MagicNode</code> is an implementation of the Construct operator.
 */

public class MagicNode extends MagicBaseNode implements Node, Element, Attr,
		Text {

	protected final int attrIdx;
	protected final short nodeType;
	// protected Node myNode;
	protected Element myEltNode;

	public MagicNode(int attrIdx, MagicBaseNode root, short nodeType) {
		super(root);
		this.attrIdx = attrIdx;
		this.nodeType = nodeType;
		myEltNode = null;
	}

	public void setYourNode(Tuple cTuple) {
		myEltNode = (Element) cTuple.getAttribute(attrIdx);
	}

	// NODE INTERFACE METHODS:

	public Node cloneNode(boolean deep) {
		assert false : "don't want to do any cloning";
		if (myEltNode != null)
			return myEltNode.cloneNode(deep);
		else {
			System.out.println("KT WARNING MAGIC CLONE");
			return new MagicNode(this.attrIdx, this.root, this.nodeType);
		}
	}

	public NamedNodeMap getAttributes() {
		return myEltNode.getAttributes();
	}

	public NodeList getChildNodes() {
		return myEltNode.getChildNodes();
	}

	public Node getFirstChild() {
		return myEltNode.getFirstChild();
	}

	public Node getLastChild() {
		return myEltNode.getLastChild();
	}

	public String getLocalName() {
		return myEltNode.getLocalName();
	}

	public String getNamespaceURI() {
		return myEltNode.getNamespaceURI();
	}

	public Node getNextSibling() {
		return null; // myEltNode.getNextSibling();
	}

	public String getNodeName() {
		return myEltNode.getNodeName();
	}

	public short getNodeType() {
		assert myEltNode == null || myEltNode.getNodeType() == nodeType : "Bad node type!!";
		return nodeType;
	}

	public String getNodeValue() throws DOMException {
		return myEltNode.getNodeValue();
	}

	public String getPrefix() {
		return myEltNode.getPrefix();
	}

	public Node getPreviousSibling() {
		return null; // return myEltNode.getPreviousSibling();
	}

	public boolean hasAttributes() {
		return myEltNode.hasAttributes();
	}

	public boolean hasChildNodes() {
		return myEltNode.hasChildNodes();
	}

	public boolean isSupported(String feature, String version) {
		return myEltNode.isSupported(feature, version);
	}

	public void normalize() {
		myEltNode.normalize();
	}

	// ELEMENT INTERFACE METHODS
	public String getAttribute(String name) {
		return myEltNode.getAttribute(name);
	}

	public Attr getAttributeNode(String name) {
		return myEltNode.getAttributeNode(name);
	}

	public Attr getAttributeNodeNS(String namespaceURI, String localName) {
		return myEltNode.getAttributeNodeNS(namespaceURI, localName);
	}

	public String getAttributeNS(String namespaceURI, String localName) {
		return myEltNode.getAttributeNS(namespaceURI, localName);
	}

	public NodeList getElementsByTagName(String name) {
		return myEltNode.getElementsByTagName(name);
	}

	public NodeList getElementsByTagNameNS(String namespaceURI, String localName) {
		return myEltNode.getElementsByTagNameNS(namespaceURI, localName);
	}

	public String getTagName() {
		return myEltNode.getTagName();
	}

	public boolean hasAttribute(String name) {
		return myEltNode.hasAttribute(name);
	}

	public boolean hasAttributeNS(String namespaceURI, String localName) {
		return myEltNode.hasAttributeNS(namespaceURI, localName);
	}

	public void removeAttribute(String name) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void removeAttributeNS(String namespaceURI, String localName)
			throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setAttribute(String name, String value) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public Attr setAttributeNode(Attr newAttr) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setAttributeNS(String namespaceURI, String qualifiedName,
			String value) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	// ATTR INTERFACE METHODS
	public String getName() {
		return ((Attr) myEltNode).getName();
	}

	public Element getOwnerElement() throws DOMException {
		assert false : "Get owner element not supported on Magic Nodes";
		throw new DOMException(DOMException.NOT_SUPPORTED_ERR,
				"Get owner element not supported on Magic Nodes");
	}

	public boolean getSpecified() {
		return ((Attr) myEltNode).getSpecified();
	}

	public String getValue() {
		return ((Attr) myEltNode).getValue();
	}

	public void setValue(String value) throws DOMException {
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	// TEXT INTERFACE METHODS
	public Text splitText(int offset) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	// CHARACTER DATA INTERFACE METHODS
	public void appendData(String arg) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void deleteData(int offset, int count) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public String getData() throws DOMException {
		return ((CharacterData) myEltNode).getData();
	}

	public int getLength() {
		return ((CharacterData) myEltNode).getLength();
	}

	public void insertData(int offset, String arg) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void replaceData(int offset, int count, String arg)
			throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public void setData(String data) throws DOMException {
		assert false : "Magic objects are read-only";
		throw new DOMException(DOMException.NO_MODIFICATION_ALLOWED_ERR,
				"Magic objects are read-only.");
	}

	public String substringData(int offset, int count) throws DOMException {
		return ((CharacterData) myEltNode).substringData(offset, count);
	}

	// SUPPORT FUNCTIONS
	/*
	 * protected Node getNodeFromTuple() { assert root != null :
	 * "KT: Root is null!!"; if(root.getCurrentTuple() == null) return null;
	 * else return root.getCurrentTuple().getAttribute(attrIdx);
	 * 
	 * }
	 */

	public TypeInfo getSchemaTypeInfo() {
		throw new PEException("Not implemented yet");
	}

	public void setIdAttribute(String name, boolean isId) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public void setIdAttributeNode(Attr idAttr, boolean isId)
			throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public void setIdAttributeNS(String namespaceURI, String localName,
			boolean isId) throws DOMException {
		throw new PEException("Not implemented yet");
	}

	public boolean isId() {
		throw new PEException("Not implemented yet");
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
