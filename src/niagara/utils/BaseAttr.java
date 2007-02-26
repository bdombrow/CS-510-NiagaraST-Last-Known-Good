package niagara.utils;

import org.w3c.dom.Node;

public abstract class BaseAttr {
	
	protected Object attrVal;
	/*
	 * punct indicates that the BaseAttr object contains a special value, a punctuation pattern;
	 * when punct is true, the attrVal is a String object, no matter what data type of the object is;   
	 */
	protected boolean punct = false;
	
	public abstract void loadFromXMLNode(Node XMLNode);
	public abstract void loadFromObject(Object value);
	public abstract void loadPunctFromXMLNode(Node XMLNode);
	
	public abstract String toASCII();
	
	public abstract boolean eq(BaseAttr other);
	public abstract BaseAttr copy();
	public abstract boolean gt(BaseAttr other);
	public abstract boolean lt(BaseAttr other);

	public static DataType getDataTypeFromString(String typeName) {
		DataType dataType;
        if (typeName.equalsIgnoreCase("string"))
        	dataType = DataType.String;
        else if (typeName.equalsIgnoreCase("xml"))
        	dataType = DataType.XML;
        else if (typeName.equalsIgnoreCase("integer") ||
        		 typeName.equalsIgnoreCase("int"))
        	dataType = DataType.Integer;
        else if (typeName.equalsIgnoreCase("timestamp") ||
        		 typeName.equalsIgnoreCase("ts"))
        	dataType = DataType.TS;
        else
        	throw new PEException ("JL: Unsupported data type: " + typeName + " :");
        return dataType;

	}
	
	public static enum Type {Int, String, XML, Timestamp};
}