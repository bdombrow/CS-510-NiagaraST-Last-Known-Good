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

public class FileScanOp extends unryOp {

    private FileScanSpec fileScanSpec;

    /**
     * Method for initializing the stream scan operator. 
     *
     * @param spec A completed FileSpec object with all
     *             the specifications necessary for the stream.
     */
    public void setFileScan(FileScanSpec spec) {
	fileScanSpec = spec;
    }
    
    /**
     * Returns the specification for this stream scan
     *
     * @return The specification for this stream as a FileSpec
     *         object
     */

    public FileScanSpec getSpec() {
	return fileScanSpec;
    }

    public void dump() {
	System.out.println("FileScan Operator: ");
	fileScanSpec.dump(System.out);
	System.out.println("Selected Algo " + 
			   String.valueOf(selectedAlgorithmIndex));
	System.out.println();
    }

    public boolean isSourceOp() {
	return true;
    }
}


