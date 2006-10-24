package niagara.utils;

import org.w3c.dom.Node;

public abstract class BaseAttr {
	
	protected Object attrVal;
	
	public abstract void loadFromXMLNode(Node XMLNode);
	public abstract void loadFromObject(Object value);
	
	public abstract String toASCII();
	
	public abstract boolean eq(BaseAttr other);
	public abstract BaseAttr copy();
	public abstract boolean gt(BaseAttr other);
	public abstract boolean lt(BaseAttr other);
	
}