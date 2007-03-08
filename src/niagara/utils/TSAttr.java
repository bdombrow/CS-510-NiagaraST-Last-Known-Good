package niagara.utils;

import org.w3c.dom.Node;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;

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
	
	public void loadPunctFromXMLNode (Node xmlNode) {
		loadFromXMLNode(xmlNode);
		punct = true;
	}
	
	public void loadFromObject(Object value) {
	
		if (value instanceof Long)
			attrVal = value;
		else if (value instanceof String)
			attrVal = Long.valueOf((String)value);
		else 
			throw new PEException("JL: Unsupported Attribute Data Type");
	}
	
	public String toASCII() {
		return ((Long)attrVal).toString();
	}
	
	/*
	 * As we assume punctuation on time attribute, and thus we assume that wildstar 
	 * won't appear in a TSAttr, even if it is a punctuation   
	 * 
	 */
	public boolean eq(BaseAttr other) {
		if (!(other instanceof TSAttr))
			return false;
		return (((Long)attrVal).compareTo((Long)other.attrVal) == 0);
	}
	
	public boolean gt(BaseAttr other) {
		if (other instanceof TSAttr)
			return (((Long)attrVal).compareTo((Long)other.attrVal) > 0);
		else 
			throw new PEException("JL: Comparing Different Attribute Types");
	}

	public boolean lt(BaseAttr other) {
		if (other instanceof TSAttr)
			return (((Long)attrVal).compareTo((Long)other.attrVal) < 0);
		else 
			throw new PEException("JL: Comparing Different Attribute Types");
	}
	
	public int extractMinute() {
		Calendar calender = GregorianCalendar.getInstance();
		calender.setTime(new Date((Long)attrVal));
		return calender.get(Calendar.MINUTE);		
	}
	
	public int extractHour() {
		Calendar calender = GregorianCalendar.getInstance();
		calender.setTime(new Date((Long)attrVal));
		return calender.get(Calendar.HOUR_OF_DAY);		
	}

	public int extractDay() {
		Calendar calender = GregorianCalendar.getInstance();
		calender.setTime(new Date((Long)attrVal));
		return calender.get(Calendar.DAY_OF_WEEK);		
	}

  public long extractEpoch() {
    return ((Long)attrVal).longValue();
  }

	public Long map(int factor) {
		return (Long)attrVal/factor;
	} 
	
	public BaseAttr copy() {
		return new IntegerAttr(attrVal);		
	}

}
