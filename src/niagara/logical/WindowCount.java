package niagara.logical;

import java.util.ArrayList;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;

import org.w3c.dom.Element;

/**
 * This is the class for the count operator, that is a type of group operator.
 * 
 * @version 1.0
 * 
 */

@SuppressWarnings("unchecked")
public class WindowCount extends WindowAggregate {

	public void dump() {
		super.dump("windowCountOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		ArrayList aggrAttrNames = new ArrayList();
		aggrAttrNames.add("countattr");

		super.loadFromXML(e, inputProperties, aggrAttrNames);
	}

	protected WindowAggregate getInstance() {
		return new WindowCount();
	}
}
