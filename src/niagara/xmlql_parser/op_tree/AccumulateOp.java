/*
 * $Id: AccumulateOp.java,v 1.7 2002/10/27 01:20:21 vpapad Exp $
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

import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.query_engine.MergeTree;
import niagara.query_engine.MTException;

public class AccumulateOp extends unryOp {

    private MergeTree mergeTree;
    private Attribute mergeAttr;
    private String accumFileName;
    private String initialAccumFile;
    boolean clear; /* clear existing accum file or not */

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
    public void setAccumulate(String _mergeTemplate, 
			      Attribute _mergeAttr,
			      String _accumFileName, 
			      String _initialAccumFile,
			      boolean _clear) 
	throws MTException {
	mergeAttr = _mergeAttr;
	mergeTree = new MergeTree();
	accumFileName = _accumFileName;
	initialAccumFile = _initialAccumFile;
	clear = _clear;

	/* true indicates that accumulate constraints should be checked */
	mergeTree.create(_mergeTemplate, true);
	return;
    }

    /** 
     * Returns the mergeTree for this operator.
     */
    public MergeTree getMergeTree() {
	return mergeTree;
    }

    public Attribute getMergeAttr() {
	return mergeAttr; 
    }

    public String getAccumFileName() {
	return accumFileName;
    }

    public String getInitialAccumFile() {
	return initialAccumFile;
    }

    public boolean getClear() {
	return clear;
    }
    
    public void dump() {
	System.out.println("Accumulate Operator: ");
	mergeTree.dump(System.out);
	System.out.println("MergeIndex " + mergeAttr.getName());
	if(accumFileName != null) {
	    System.out.println("AccumFileName " + accumFileName);
	}
	System.out.println("Selected Algo " + 
			   String.valueOf(selectedAlgorithmIndex));
	System.out.println();
    }
}
