package niagara.utils;

import org.w3c.dom.Node;

public class LongAttr extends BaseAttr implements Arithmetics{
	 
	public static String name = "long";
	
	public LongAttr () {};
	
	public LongAttr (Node XMLNode) {
		loadFromXMLNode(XMLNode);
	}
	
	public LongAttr (Object value) {
		loadFromObject(value);
	}
	
	public void loadFromXMLNode(Node xmlNode) {
		short type = xmlNode.getNodeType();
		
		switch (type) {
		case Node.ELEMENT_NODE:
			attrVal = Long.valueOf(xmlNode.getFirstChild().getNodeValue());
			break;
		case Node.ATTRIBUTE_NODE:
			attrVal = Long.valueOf(xmlNode.getNodeValue());
			break;
		case Node.TEXT_NODE:
			attrVal = Long.valueOf(xmlNode.getNodeValue());
			break;
		default:
			throw new PEException("JL: Unsupported XML Node Type");
		}
	}

	public void loadPunctFromXMLNode(Node xmlNode) {
		short type = xmlNode.getNodeType();
		String nodeVal;
		
		switch (type) {
		case Node.ELEMENT_NODE:
			nodeVal = xmlNode.getFirstChild().getNodeValue();
			break;
		case Node.ATTRIBUTE_NODE:
			nodeVal = xmlNode.getNodeValue();
			break;
		case Node.TEXT_NODE:
			nodeVal = xmlNode.getNodeValue();
			break;
		default:
			nodeVal = null;
			throw new PEException("JL: Unsupported XML Node Type");
		}
		/*
		 * Right now, we only handle "*" for punctuation pattern
		 */
		if (nodeVal != null) {
			if (nodeVal.startsWith("*")) 
				attrVal = nodeVal;
			else
				attrVal = Integer.valueOf(nodeVal);
		}
		punct = true;

	}

	public void loadFromObject(Object value) {
	
		if (value instanceof Long)
			attrVal = value;
		else if (value instanceof Integer)
			attrVal = value;
		else if (value instanceof String)
			attrVal = Long.parseLong((String)value);
		else
			throw new PEException("JL: Unsupported Attribute Data Type");
	}
	
	public void loadFromValue(int value) {
		attrVal = new Long(value);
	}
	
	public String toASCII() {
		return attrVal.toString(); 
	}
	
	public boolean eq(BaseAttr other) {
		if (!(other instanceof LongAttr))
			return false;
		
		return attrVal.equals(other.attrVal);
	}
	
	public boolean gt(BaseAttr other) {
		if (!(other instanceof LongAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		if (((Long)attrVal) > (Long)other.attrVal) 
			return true;
		else 
			return false;
	}

	public boolean lt(BaseAttr other) {
		if (!(other instanceof LongAttr))
			throw new PEException ("JL: Comparing to a different data type");

		if (((Long)attrVal) < (Long)other.attrVal)
			return true;
		else 
			return false;

	}
	
	public BaseAttr copy() {
		return new LongAttr(attrVal);		
	}
	
	public LongAttr plus (BaseAttr other) {
		if (!(other instanceof LongAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new LongAttr((Long)attrVal + (Long)other.attrVal);
	}

	public LongAttr minus (BaseAttr other) {
		if (!(other instanceof LongAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new LongAttr((Long)attrVal - (Long)other.attrVal);
	}

	public LongAttr multiply (BaseAttr other) {
		if (!(other instanceof LongAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new LongAttr((Long)attrVal * (Long)other.attrVal);
	}

	public LongAttr divid (BaseAttr other) {
		if (!(other instanceof LongAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new LongAttr((Long)attrVal / (Long)other.attrVal);
	}

}