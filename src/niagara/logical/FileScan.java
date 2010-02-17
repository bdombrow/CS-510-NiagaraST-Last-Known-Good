package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * This class represents stream Scan operator that fetches data from a stream,
 * parses it and returns the DOM tree to the operator above it.
 */

public class FileScan extends Stream {
	// Required zero-argument constructor
	public FileScan() {
	}

	public FileScan(FileScanSpec fileScanSpec, Attribute variable) {
		this.streamSpec = fileScanSpec;
		this.variable = variable;
	}

	/**
	 * Returns the specification for this stream scan
	 * 
	 * @return The specification for this stream as a FileSpec object
	 */

	public void dump() {
		System.out.println("FileScan Operator: ");
		streamSpec.dump(System.out);
		System.out.println();
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return new LogicalProperty(1, new Attrs(variable), true);
	}

	public Op opCopy() {
		return new FileScan((FileScanSpec) streamSpec, variable);
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof FileScan))
			return false;
		if (obj.getClass() != FileScan.class)
			return obj.equals(this);
		FileScan other = (FileScan) obj;
		return streamSpec.equals(other.streamSpec)
				&& variable.equals(other.variable);
	}

	public int hashCode() {
		return streamSpec.hashCode() ^ variable.hashCode();
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		boolean isStream = e.getAttribute("isstream").equalsIgnoreCase("yes");
		int delay = Integer.parseInt(e.getAttribute("delay"));
		if (!isStream && delay > 0) {
			throw new InvalidPlanException(
					"delay > 0 allowed only if isstream is yes");
		}
		streamSpec = new FileScanSpec(e.getAttribute("filename"), isStream,
				delay);
		variable = new Variable(e.getAttribute("id"));
	}
}
