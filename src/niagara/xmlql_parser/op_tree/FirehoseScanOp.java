// $Id: FirehoseScanOp.java,v 1.7 2002/10/31 04:17:05 vpapad Exp $

package niagara.xmlql_parser.op_tree;

/**
 * This class represents firehose Scan operator that fetches data
 * from a firehose, parses it and returns the DOM tree to the operator
 * above it. 
 */

import java.util.*;

import niagara.logical.NullaryOp;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.firehose.FirehoseSpec;

public class FirehoseScanOp extends NullaryOp {
    private FirehoseSpec fhSpec;

    private Attribute variable;
    
    // Required zero-argument constructor
    public FirehoseScanOp() {}
    
    public FirehoseScanOp(FirehoseSpec fhSpec, Attribute variable) {
        setFirehoseScan(fhSpec, variable);
    }
    

    /**
     * Method for initializing the firehose scan operator. See
     * the FirehoseSpec constructor for descriptions of the
     * FirehoseSpec variables
     *
     * @param spec A completed FirehoseSpec object with all
     *             the specifications necessary for the firehose.
     */
    public void setFirehoseScan(FirehoseSpec spec, Attribute variable) {
	fhSpec = spec;
        this.variable = variable;
    }
    
    /**
     * Returns the specification for this firehose scan - contains information
     * like listener port number, listener host name, etc
     *
     * @return The specification for this firehose as a FirehoseSpec
     *         object
     */

    public FirehoseSpec getSpec() {
	return fhSpec;
    }

    public void dump() {
	System.out.println("FirehoseScan Operator: ");
	fhSpec.dump(System.out);
	System.out.println();
    }

    public boolean isSourceOp() {
	return true;
    }
    
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return new LogicalProperty(
            1,
            new Attrs(variable),
            true);
    }
    
    public Op copy() {
        return new FirehoseScanOp(fhSpec, variable);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof FileScanOp)) return false;
        if (obj.getClass() != FileScanOp.class) return obj.equals(this);
        FirehoseScanOp other = (FirehoseScanOp) obj;
        return fhSpec.equals(other.fhSpec) && variable.equals(other.variable);
    }

    public int hashCode() {
        return fhSpec.hashCode() ^ variable.hashCode();
    }
}


