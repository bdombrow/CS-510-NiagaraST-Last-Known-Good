// $Id: FileScanOp.java,v 1.3 2002/12/10 00:51:53 vpapad Exp $

package niagara.xmlql_parser.op_tree;

/**
 * This class represents stream Scan operator that fetches data
 * from a stream, parses it and returns the DOM tree to the operator
 * above it. 
 */

import java.util.*;

import niagara.logical.*;
import niagara.logical.NodeDomain;
import niagara.logical.Variable;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;

public class FileScanOp extends StreamOp {
    // Required zero-argument constructor
    public FileScanOp() {}
    
    public FileScanOp(FileScanSpec fileScanSpec, Attribute variable) {
        setFileScan(fileScanSpec, variable);
    }
    
    /**
     * Method for initializing the stream scan operator. 
     *
     * @param spec A completed FileSpec object with all
     *             the specifications necessary for the stream.
     */
    public void setFileScan(FileScanSpec spec, Attribute variable) {
	streamSpec = spec;
        this.variable = variable;
    }
    
    /**
     * Returns the specification for this stream scan
     *
     * @return The specification for this stream as a FileSpec
     *         object
     */

    public void dump() {
	System.out.println("FileScan Operator: ");
	streamSpec.dump(System.out);
	System.out.println();
    }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        return new LogicalProperty(
            1,
            new Attrs(variable),
            true);
    }
    
    public Op copy() {
        return new FileScanOp((FileScanSpec) streamSpec, variable);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof FileScanOp)) return false;
        if (obj.getClass() != FileScanOp.class) return obj.equals(this);
        FileScanOp other = (FileScanOp) obj;
        return streamSpec.equals(other.streamSpec) && variable.equals(other.variable);
    }

    public int hashCode() {
        return streamSpec.hashCode() ^ variable.hashCode();
    }
}


