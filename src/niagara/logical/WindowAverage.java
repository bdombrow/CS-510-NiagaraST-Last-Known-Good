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
public class WindowAverage extends WindowAggregate {

	public void dump() {
		super.dump("windowAverageOp");
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		ArrayList aggrAttrNames = new ArrayList();
		String attrName1, attrName2;

		attrName1 = e.getAttribute("avgattr");
		if (attrName1 != "") {
			aggrAttrNames.add("avgattr");
		} else {
			attrName1 = e.getAttribute("sumattr");
			attrName2 = e.getAttribute("countattr");
			if (attrName1 != "" && attrName2 != "") {
				aggrAttrNames.add("sumattr");
				aggrAttrNames.add("countattr");
			} else {
				throw new InvalidPlanException(
						"no aggregate attribute specified");
			}
		}
		super.loadFromXML(e, inputProperties, aggrAttrNames);
	}

	protected WindowAggregate getInstance() {
		return new WindowAverage();
	}
}
