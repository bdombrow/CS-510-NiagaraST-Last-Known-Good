package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * The class <code>dupOp</code> is the class for operator Duplicate.
 * 
 * @version 1.0
 * 
 * @see op
 */

public class Duplicate extends UnaryOperator {

	private int numDestinationStreams;

	public void addDestinationStreams() {
		numDestinationStreams++;
	};

	public void dump() {
		System.out.println("dupOp");
	}

	public int getNumberOfOutputs() {
		return numDestinationStreams;
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return input[0].copy();
	}

	public Op opCopy() {
		Duplicate op = new Duplicate();
		op.numDestinationStreams = numDestinationStreams;
		return op;
	}

	public int hashCode() {
		return numDestinationStreams;
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Duplicate))
			return false;
		if (obj.getClass() != Duplicate.class)
			return obj.equals(this); 
		Duplicate other = (Duplicate) obj;
		return numDestinationStreams == other.numDestinationStreams;
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String branchAttr = e.getAttribute("branch");
		// XXX vpapad: catch format exception, check that we really have
		// that many output streams - why do we have to specify this here?
		numDestinationStreams = Integer.parseInt(branchAttr);
	}
}
