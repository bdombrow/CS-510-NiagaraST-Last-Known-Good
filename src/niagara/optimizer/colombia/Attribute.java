package niagara.optimizer.colombia;

import niagara.utils.BaseAttr;

/** Tuple attributes */
public interface Attribute {
	String getName();
	
	void setName(String name);

	Domain getDomain();

	Attribute copy();

	BaseAttr.Type getDataType();

	boolean matchesName(String name);
}
