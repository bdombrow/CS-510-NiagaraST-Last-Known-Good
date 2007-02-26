package niagara.utils;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLAttr extends BaseAttr {
	
	public XMLAttr () {};
	
	public XMLAttr (Node XMLNode) {
		loadFromXMLNode(XMLNode);
	}
	
	public XMLAttr (Object value) {
		loadFromObject(value);
	}
	
	public void loadFromXMLNode(Node xmlNode) {
		attrVal = xmlNode;
	}
	
	public void loadPunctFromXMLNode (Node xmlNode) {
		loadFromXMLNode (xmlNode);
		punct = true;
	}
	
	public void loadFromObject(Object value) {
		throw new PEException("JL: Unsupported yet");
	}
	
	public String toASCII() {
		short type = ((Node)attrVal).getNodeType();
		
		switch (type) {
		case Node.ATTRIBUTE_NODE:
			return ((Node)attrVal).getNodeValue();
		case Node.ELEMENT_NODE:
			return ((Node)attrVal).getFirstChild().getNodeValue();
			
		case Node.TEXT_NODE:
			return ((Node)attrVal).getNodeValue();
		default:
			throw new PEException ("JL: Don't know how to handle this type of XML node");
		}

	}
	
	public boolean eq(BaseAttr other) {
		if (!(other instanceof XMLAttr))
			return false;
		
		return nodeEquals((Node)attrVal, (Node)other.attrVal);
	}
	
    private boolean nodeEquals(Node nd1, Node nd2) {
    	if (!nd1.getNodeName().equals(nd2.getNodeName()))
    	    return false;

    	if (nd1.getNodeType() != nd2.getNodeType())
    	    return false;

    	String stValue = nd1.getNodeValue();
    	if (stValue == null) {
    	    if (nd2.getNodeValue() != null)
    		return false;
    	} else {
    	    if (!stValue.equals(nd2.getNodeValue()))
    		return false;
    	}
    	NodeList nl1 = nd1.getChildNodes();
    	NodeList nl2 = nd2.getChildNodes();
    	if (nl1.getLength() != nl2.getLength())
    	    return false;

    	boolean fEquals = true;
    	for (int i=0; i < nl1.getLength() && fEquals; i++) {
    	    fEquals = nodeEquals(nl1.item(i), nl2.item(i));
    	}

    	return fEquals;
    }

	
	public boolean gt(BaseAttr other) {
		if (!(other instanceof XMLAttr))
			throw new PEException("JL: Comparing to a different data type");
		return (this.toASCII().compareTo(other.toASCII()) > 0);

	}

	public boolean lt(BaseAttr other) {
		if (!(other instanceof XMLAttr))
			throw new PEException("JL: Comparing to a different data type");
		return (this.toASCII().compareTo(other.toASCII()) < 0);
	}
	
	public BaseAttr copy() {
		return new XMLAttr (((Node)attrVal).cloneNode(true));
	}
	
	public Node getNodeValue() {
		return (Node)attrVal;
	}

}