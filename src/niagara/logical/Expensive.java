package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * @author rfernand
 * @version 1.0
 * 
 * The <code>Expensive</code> logical operator is the identity, except it has a fixed cost processing per tuple.
 *
 */
public class Expensive extends UnaryOperator {

	private long cost;

	public Expensive() {
		cost = 0;
	}

	public long getCost() {
		return cost;
	}


	public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog) throws InvalidPlanException {
		cost = Long.parseLong(e.getAttribute("cost"));
	}


	@Override
	public boolean equals(Object other) {

		if(other == null || !(other instanceof Expensive))
			return false;
		if(other.getClass() != Expensive.class)
			return other.equals(this);

		Expensive ot = (Expensive)other;

		if(ot.cost != this.cost) 
		return false;

		return true;

	}

	public void dump() {
		System.out.println("Expensive :");
		System.out.println("Cost: " + String.valueOf(cost));
	}

	public String toString() {
		return " expensive " ;
	}

	@Override
	public int hashCode() {
		return String.valueOf(cost).hashCode();
	}

	@Override
	public Op opCopy() {
		Expensive op = new Expensive();
		op.cost = cost;
		return op;	
	}

	@Override
	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return input[0].copy();
	}
}
