package niagara.utils;

import niagara.optimizer.colombia.Attribute;

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

	public Object attrVal(){ return attrVal; }
	
	public static BaseAttr.Type getDataTypeFromString(String typeName) {
		BaseAttr.Type dataType;
        if (typeName.equalsIgnoreCase("string"))
        	dataType = BaseAttr.Type.String;
        else if (typeName.equalsIgnoreCase("xml"))
        	dataType = BaseAttr.Type.XML;
        else if (typeName.equalsIgnoreCase("integer") ||
        		 typeName.equalsIgnoreCase("int"))
        	dataType = BaseAttr.Type.Int;
        else if (typeName.equalsIgnoreCase("long")) 
        	dataType = BaseAttr.Type.Long;
        else if (typeName.equalsIgnoreCase("timestamp") ||
        		 typeName.equalsIgnoreCase("ts"))
        	dataType = BaseAttr.Type.TS;
        else
        	throw new PEException ("JL: Unsupported data type: " + typeName + " :");
        return dataType;

	}
	
	public static enum Type {Int, Long, String, XML, TS};
	
	public static BaseAttr createWildStar(Type attrType) {
		BaseAttr attr = null; 
		if (Type.Int.compareTo(attrType) == 0) 
			attr = new IntegerAttr();
		else if (Type.Long.compareTo(attrType) == 0)
			attr = new LongAttr();
		else if (Type.XML.compareTo(attrType) == 0)
			attr = new XMLAttr();
		else if (Type.String.compareTo(attrType) == 0)
			attr = new StringAttr();
		else if (Type.TS.compareTo(attrType) == 0)
			attr = new TSAttr();
        else
        	throw new PEException ("JL: Unsupported data type: " + attrType.toString() + " :");

		attr.wildStar();
		return attr;
	}

	public static BaseAttr createAttr(Tuple inputTuple, Node n, 
			Attribute variable) {
		BaseAttr attr;
		BaseAttr.Type type = variable.getDataType(); 

		switch (type) {
		case String: 
			attr = new StringAttr();
			break;
		case Int: 
			attr = new IntegerAttr();
			break;
		case Long: 
			attr = new LongAttr();
			break;
		case TS: 
			attr = new TSAttr();
			break;
		case XML: 
			attr = new XMLAttr();
			break;
		default: 
			throw new PEException ("JL: Unsupported data type");
		}
		if (inputTuple != null)
			attr.loadFromXMLNode(n);
		else
			attr.loadPunctFromXMLNode(n);
		return attr;
	}
	
	private void wildStar() {
		attrVal = new String("*");
		// punct is true when the attrVal of a BaseAttr contrains a "*"
		punct = true;
	}
	
	

}
