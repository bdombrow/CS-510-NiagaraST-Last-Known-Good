package niagara.utils;

import org.w3c.dom.Node;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TSAttr extends BaseAttr {
	
  boolean outputInDateFormat = false;
  
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
		//punct = true;
	}
	
	public void loadFromObject(Object value) 
	{
	
		if (value instanceof Long)
			attrVal = value;
		else if (value instanceof String)
		{
		  String ts = (String) value;
		  try
		  {
		    attrVal = Long.valueOf(ts);
		  }
		  catch (NumberFormatException e)
		  {
  		  // If the string is in "2010-08-09 00:00:00-07" format, need to convert it to long format
  		  Date date = null;
  		  try
  		  {
    		  Pattern datePattern = Pattern.compile("(19|20)\\d{2}-(0[1-9]|1[012]|[1-9])-(0[1-9]|[1-9]|[12][0-9]|3[01])");

    		  Matcher dateMatcher = datePattern.matcher(ts);
    		  if (dateMatcher.find())
    		  {
    		    // Input contains a date
    		    
    		    // If the ts value includes the timezone or surrounding quotes Date doesn't like that
    		    if (ts.endsWith("-07"))
    		    {
    		      ts = ts.substring(0, ts.length()-3);
    		    }
    		    else if(ts.endsWith("-07\""))
    		    {
    		      ts = ts.substring(1, ts.length()-4);
    		    }
    		    else if(ts.startsWith("\"") && ts.endsWith("\""))
            {
              ts = ts.substring(1, ts.length()-1);
            }
    		    
    		    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	      date = df.parse(ts);
    	      
    	      attrVal = date.getTime();
    		  }
    		  else
    		  {
    		    throw new PEException("Invalid TS Attribute Data, cannot parse");
    		  }
  		  }
  		  catch (PEException e2)
  		  {
  		    e2.printStackTrace();
  		  }
        catch (ParseException e3)
        {
          // TODO Auto-generated catch block
          e3.printStackTrace();
        }
		  }
		}
		else 
			throw new PEException("JL: Unsupported Attribute Data Type");
	}
	
	public String toASCII() {
		if (punct)
		{
			return (String) attrVal;
		}
		else if (outputInDateFormat == true)
		{

	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
      Date date = new Date((Long) attrVal);
      return df.format(date);

		}
		return ((Long)attrVal).toString();
	}
	
	/*
	 * As we assume punctuation on time attribute, and thus we assume that wildstar 
	 * won't appear in a TSAttr, even if it is a punctuation   
	 * 
	 * JMW: Not sure about that, and not sure about these comparison functions
	 */
	public boolean eq(BaseAttr other) {
	  if (!((other instanceof TSAttr) || (other instanceof LongAttr)))
			return false;
		
		return ((Long)attrVal).compareTo((Long)other.attrVal) == 0;
	}
	
	public boolean gt(BaseAttr other) {
		//if (!((other instanceof TSAttr) || (other instanceof LongAttr)))
	  if (((other instanceof TSAttr) || (other instanceof LongAttr)))
			return ((Long)attrVal).compareTo((Long)other.attrVal) > 0;
		
		else 
			throw new PEException("JL: Comparing Different Attribute Types");
	}

	public boolean lt(BaseAttr other) {
		//if (!((other instanceof TSAttr) || (other instanceof LongAttr)))
	  if (((other instanceof TSAttr) || (other instanceof LongAttr)))
			return ((Long)attrVal).compareTo((Long)other.attrVal) < 0;
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

	/*
	 * the factor should be specified in the same granularity of the timestamp, e.g., second
	 */
  public long extractEpoch() {
    return ((Long)attrVal).longValue();
  }

	public Long map(int factor) {
		return (Long)attrVal/factor;
	} 
	
	public BaseAttr copy() {
		return new IntegerAttr(attrVal);		
	}
	
	public String toString() {
	  if (punct)
    {
      return (String) attrVal;
    }
    else if (outputInDateFormat == true)
    {

      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
      Date date = new Date((Long) attrVal);
      return df.format(date);
    }
	  return ((Long)attrVal).toString();
  }

}
