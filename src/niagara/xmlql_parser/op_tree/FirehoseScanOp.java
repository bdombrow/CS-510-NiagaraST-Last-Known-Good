/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.xmlql_parser.op_tree;

/**
 * This class represents firehose Scan operator that fetches data
 * from a firehose, parses it and returns the DOM tree to the operator
 * above it. 
 */

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.firehose.FirehoseSpec;

public class FirehoseScanOp extends unryOp {

    private FirehoseSpec fhSpec;

    /**
     * Method for initializing the firehose scan operator. See
     * the FirehoseSpec constructor for descriptions of the
     * FirehoseSpec variables
     *
     * @param spec A completed FirehoseSpec object with all
     *             the specifications necessary for the firehose.
     */
    public void setFirehoseScan(FirehoseSpec spec) {
	fhSpec = spec;
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
	System.out.println("Selected Algo " + 
			   String.valueOf(selectedAlgorithmIndex));
	System.out.println();
    }

    public boolean isSourceOp() {
	return true;
    }
}


