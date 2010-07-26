package niagara.utils;

import org.w3c.dom.Node;

public class IntegerAttr extends BaseAttr implements Arithmetics{
	 
	public static String name = "integer";
	
	public IntegerAttr () {};
	
	public IntegerAttr (Node XMLNode) {
		loadFromXMLNode(XMLNode);
	}
	
	public IntegerAttr (Object value) {
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
	
		if (value instanceof Integer)
			attrVal = value;
		else if (value instanceof String)
			attrVal = Integer.parseInt((String)value);
		else
			throw new PEException("JL: Unsupported Attribute Data Type");
	}
	
	public void loadFromValue(int value) {
		attrVal = new Integer(value);
	}
	
	public String toASCII() {
		return attrVal.toString(); 
	}
	
	public boolean eq(BaseAttr other) {
		if (other.punct || punct) {
			throw new PEException("JL: Don't know how to do comparison with punctuation");
/*		Proposed fix?	
 * String value = (String)other.attrVal;
 * return value.equals(attrVal.toString());
 * */
		}

		if (!(other instanceof IntegerAttr))
			return false;
			
		return (((Integer)attrVal).compareTo((Integer)other.attrVal) == 0);
	}
	
	public boolean gt(BaseAttr other) {
		if (other.punct || punct) {
			throw new PEException("JL: Don't know how to do comparison with punctuation");
		}

		if (!(other instanceof IntegerAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return (((Integer)attrVal).compareTo((Integer)other.attrVal) > 0);
	}

	public boolean lt(BaseAttr other) {
		if (other.punct || punct) {
			throw new PEException("JL: Don't know how to do comparison with punctuation");
		}

		if (!(other instanceof IntegerAttr))
			throw new PEException ("JL: Comparing to a different data type");

		return (((Integer)attrVal).compareTo((Integer)other.attrVal) < 0);
	}
	
	public BaseAttr copy() {
		return new IntegerAttr(attrVal);		
	}
	
	public IntegerAttr plus (BaseAttr other) {
		if (!(other instanceof IntegerAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new IntegerAttr((Integer)attrVal + (Integer)other.attrVal);
	}

	public IntegerAttr minus (BaseAttr other) {
		if (!(other instanceof IntegerAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new IntegerAttr((Integer)attrVal - (Integer)other.attrVal);
	}

	public IntegerAttr multiply (BaseAttr other) {
		if (!(other instanceof IntegerAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new IntegerAttr((Integer)attrVal * (Integer)other.attrVal);
	}

	public IntegerAttr divid (BaseAttr other) {
		if (!(other instanceof IntegerAttr))
			throw new PEException ("JL: Comparing to a different data type");
		
		return new IntegerAttr((Integer)attrVal / (Integer)other.attrVal);
	}

}
