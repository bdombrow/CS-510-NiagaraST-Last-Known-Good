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

public class AccumulateOp extends unryOp {

    private MergeTree mergeTree;
    private int mergeIndex;

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
     * @param mergeTemplate A DOM Document representing a parsed XML
     *                      merge template
     * @param mergeIndex  The index in the tuple structure of the XML 
     *              documents/fragments to be merged
     */
    public void setAccumulate(Document mergeTemplate, int _mergeIndex) {
	mergeIndex = _mergeIndex;
	mergeTree = new MergeTree();
	mergeTree.create(mergeTemplate);
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

}
