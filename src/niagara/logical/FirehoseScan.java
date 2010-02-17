package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.connection_server.NiagraServer;
import niagara.firehose.FirehoseConstants;
import niagara.firehose.FirehoseSpec;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;

/**
 * This class represents firehose Scan operator that fetches data from a
 * firehose, parses it and returns the DOM tree to the operator above it.
 */
public class FirehoseScan extends Stream {
	// Required zero-argument constructor
	public FirehoseScan() {
	}

	public FirehoseScan(FirehoseSpec fhSpec, Attribute variable) {
		this.streamSpec = fhSpec;
		this.variable = variable;
	}

	/**
	 * Returns the specification for this firehose scan - contains information
	 * like listener port number, listener host name, etc
	 * 
	 * @return The specification for this firehose as a FirehoseSpec object
	 */

	public void dump() {
		System.out.println("FirehoseScan Operator: ");
		streamSpec.dump(System.out);
		System.out.println();
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return new LogicalProperty(1, new Attrs(variable), true);
	}

	public Op opCopy() {
		return new FirehoseScan((FirehoseSpec) streamSpec, variable);
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof FirehoseScan))
			return false;
		if (obj.getClass() != FirehoseScan.class)
			return obj.equals(this);
		FirehoseScan other = (FirehoseScan) obj;
		return streamSpec.equals(other.streamSpec)
				&& variable.equals(other.variable);
	}

	public int hashCode() {
		return streamSpec.hashCode() ^ variable.hashCode();
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		String id = e.getAttribute("id");
		String host = e.getAttribute("host");
		int port = Integer.parseInt(e.getAttribute("port"));
		int rate = Integer.parseInt(e.getAttribute("rate"));
		String dataTypeStr = e.getAttribute("datatype");
		String descriptor = e.getAttribute("desc");
		String descriptor2 = e.getAttribute("desc2");

		int numGenCalls = Integer.parseInt(e.getAttribute("num_gen_calls"));
		int numTLElts = Integer.parseInt(e.getAttribute("num_tl_elts"));
		boolean prettyPrint = e.getAttribute("prettyprint").equalsIgnoreCase(
				"yes");
		String trace = e.getAttribute("trace");

		int dataType = -1;
		boolean found = false;
		for (int i = 0; i < FirehoseConstants.numDataTypes; i++) {
			if (dataTypeStr.equalsIgnoreCase(FirehoseConstants.typeNames[i])) {
				dataType = i;
				found = true;
				break;
			}
		}
		if (found == false)
			throw new InvalidPlanException("Invalid type - typeStr: "
					+ dataTypeStr);

		boolean useStreamFormat = NiagraServer.usingSAXDOM();

		streamSpec = new FirehoseSpec(port, host, dataType, descriptor,
				descriptor2, numGenCalls, numTLElts, rate, useStreamFormat,
				prettyPrint, trace);

		variable = new Variable(id);
	}
}
