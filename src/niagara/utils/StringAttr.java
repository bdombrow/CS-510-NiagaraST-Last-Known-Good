package niagara.utils;

import org.w3c.dom.Node;

public class StringAttr extends BaseAttr {
	
	public StringAttr () {};
	
	public StringAttr (Node XMLNode) {
		loadFromXMLNode(XMLNode);
	}
	
	public StringAttr (Object value) {
		loadFromObject(value);
	}
	
	public void loadFromXMLNode(Node xmlNode) {
		short type = xmlNode.getNodeType();
		
		switch (type) {
		case Node.ELEMENT_NODE:
			attrVal = xmlNode.getFirstChild().getNodeValue();
			break;
		case Node.ATTRIBUTE_NODE:
			attrVal = xmlNode.getNodeValue();
			break;
		case Node.TEXT_NODE:
			attrVal = xmlNode.getNodeValue();
			break;
		default:
			throw new PEException("JL: Unsupported XML Node Type");
		}
	}
	
	public void loadFromObject(Object value) {
	
		if (value instanceof String)
			attrVal = value;
		else 
			attrVal = String.valueOf(value);
	}
	
	public String toASCII() {
		return (String)attrVal;
	}
	
	public boolean eq(BaseAttr other) {
		if (!(other instanceof StringAttr))
			return false;
		
		return attrVal.equals(other.attrVal);
	}
	
	public boolean gt(BaseAttr other) {
		if (other instanceof StringAttr)
			if (((String)attrVal).compareTo((String)other.attrVal) > 0)
				return true;
			else 
				return false;
		else 
			throw new PEException("JL: Comparing Different Attribute Types");

	}

	public boolean lt(BaseAttr other) {
		if (other instanceof StringAttr)
			if (((String)attrVal).compareTo((String)other.attrVal) < 0)
				return true;
			else 
				return false;
		else 
			throw new PEException("JL: Comparing Different Attribute Types");

	}
	
	public BaseAttr copy() {
		return new StringAttr(attrVal);		
	}

}