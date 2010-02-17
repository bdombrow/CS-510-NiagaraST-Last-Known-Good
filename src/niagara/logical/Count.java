package niagara.logical;

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

public class Count extends Aggregate {

	public void dump() {
		super.dump("CountOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		super.loadFromXML(e, inputProperties, "countattr");
	}

	protected Aggregate getInstance() {
		return new Count();
	}
}
