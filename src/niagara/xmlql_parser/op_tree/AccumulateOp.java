/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.xmlql_parser.op_tree;

/**
 * This class is a "logical" operator with physical info for
 * the Accumulate operation.  Accumulate is a unary operator which
 * takes in a stream of XML documents or fragments and merges them
 * together to create a new XML document.
 */

import java.util.*;
import org.w3c.dom.*;

import niagara.xmlql_parser.syntax_tree.*;
import niagara.query_engine.MergeTree;
import niagara.query_engine.MTException;

public class AccumulateOp extends unryOp {

    private MergeTree mergeTree;
    private int mergeIndex;
    private String accumFileName;

    /**
     * Constructor for AccumulateOp.  Just calls the super constructor.
     *
     * @param al List of algorithms which can be used to implement this 
     *           operator.
     */
    public AccumulateOp(Class[] al) {
	super("Accumulate", al);
	mergeTree = null;
	mergeIndex = 0;
    }

    /**
     * Creates the AccumulateOp. Traverses the mergeTemplate tree and
     * creates a MergeTree object.
     *
     * @param mergeTempl The file name (or perhaps URI) where the merge
     *                   template is located
     *
     * @param mergeIndex  The index in the tuple structure of the XML 
     *              documents/fragments to be merged
     */
    public void setAccumulate(String mergeTemplateStr, int _mergeIndex,
			      String _accumFileName) 
	throws MTException {
	mergeIndex = _mergeIndex;
	mergeTree = new MergeTree();
	accumFileName = _accumFileName;

	/* true indicates that accumulate constraints should be checked */
	mergeTree.create(mergeTemplateStr, true);
	return;
    }

    /** 
     * Returns the mergeTree for this operator.
     */
    public MergeTree getMergeTree() {
	return mergeTree;
    }

    public int getMergeIndex() {
	return mergeIndex; 
    }

    public String getAccumFileName() {
	return accumFileName;
    }

    public void dump() {
	System.out.println("Accumulate Operator: ");
	mergeTree.dump(System.out);
	System.out.println("MergeIndex " + String.valueOf(mergeIndex));
	if(accumFileName != null) {
	    System.out.println("AccumFileName " + accumFileName);
	}
	System.out.println("Selected Algo " + 
			   String.valueOf(selectedAlgorithmIndex));
	System.out.println();
    }
}
