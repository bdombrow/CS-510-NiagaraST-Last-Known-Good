package niagara.utils;

import org.w3c.dom.Node;

public class TSAttr extends BaseAttr {
	
	public TSAttr () {};
	
	public TSAttr (Node XMLNode) {
		loadFromXMLNode(XMLNode);
	}
	
	public TSAttr (Object value) {
		loadFromObject(value);
	}
	
	public void loadFromXMLNode(Node xmlNode) {
		short type = xmlNode.getNodeType();
		
		switch (type) {
		case Node.ELEMENT_NODE:
			attrVal = Integer.valueOf(xmlNode.getFirstChild().getNodeValue());
			break;
		case Node.ATTRIBUTE_NODE:
			attrVal = Integer.valueOf(xmlNode.getNodeValue());
			break;
		case Node.TEXT_NODE:
			attrVal = Integer.valueOf(xmlNode.getNodeValue());
			break;
		default:
			throw new PEException("JL: Unsupported XML Node Type");
		}
	}
	
	public void loadFromObject(Object value) {
	
		if (value instanceof Integer)
			attrVal = value;
		else if (value instanceof String)
			attrVal = Integer.valueOf((String)value);
		else 
			throw new PEException("JL: Unsupported Attribute Data Type");
	}
	
	public String toASCII() {
		return ((Integer)attrVal).toString();
	}
	
	public boolean eq(BaseAttr other) {
		if (!(other instanceof TSAttr))
			return false;
		
		return attrVal.equals(other.attrVal);
	}
	
	public boolean gt(BaseAttr other) {
		if (other instanceof IntegerAttr)
			if (((Integer)attrVal) > (Integer)other.attrVal) 
				return true;
			else 
				return false;
		else 
			throw new PEException("JL: Comparing Different Attribute Types");

	}

	public boolean lt(BaseAttr other) {
		if (other instanceof IntegerAttr)
			if (((Integer)attrVal) < (Integer)other.attrVal)
				return true;
			else 
				return false;
		else 
			throw new PEException("JL: Comparing Different Attribute Types");

	}
	
	public BaseAttr copy() {
		return new IntegerAttr(attrVal);		
	}

}