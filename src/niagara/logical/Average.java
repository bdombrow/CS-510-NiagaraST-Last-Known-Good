package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.LogicalProperty;

import org.w3c.dom.Element;

/**
 * This is the class for the average operator, that is a type of group operator.
 * 
 * @version 1.0
 * 
 */

public class Average extends Aggregate {

	public void dump() {
		super.dump("AverageOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		super.loadFromXML(e, inputProperties, "avgattr");
	}

	protected Aggregate getInstance() {
		return new Average();
	}
}
