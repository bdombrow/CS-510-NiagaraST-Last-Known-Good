/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.xmlql_parser.op_tree;

/**
 * This class represents stream Scan operator that fetches data
 * from a stream, parses it and returns the DOM tree to the operator
 * above it. 
 */

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

public class StreamScanOp extends unryOp {

    private StreamSpec streamSpec;

    /**
     * Constructor
     *
     * @param algList A list of algorithms which can be used to implement
     *      this operator.  Why is this passed as a parameter??
     */
    public StreamScanOp(Class[] algList) {
	super("Stream Scan", algList);
	streamSpec = null;
    }

    /**
     * Method for initializing the stream scan operator. 
     *
     * @param spec A completed StreamSpec object with all
     *             the specifications necessary for the stream.
     */
    public void setStreamScan(StreamSpec spec) {
	streamSpec = spec;
    }
    
    /**
     * Returns the specification for this stream scan
     *
     * @return The specification for this stream as a StreamSpec
     *         object
     */

    public StreamSpec getSpec() {
	return streamSpec;
    }

    public void dump() {
	System.out.println("StreamScan Operator: ");
	streamSpec.dump(System.out);
	System.out.println("Selected Algo " + 
			   String.valueOf(selectedAlgorithmIndex));
	System.out.println();
    }

    public boolean isSourceOp() {
	return true;
    }
}


