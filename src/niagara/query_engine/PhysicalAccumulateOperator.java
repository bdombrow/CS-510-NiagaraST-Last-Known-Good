package niagara.query_engine;

import java.util.Vector;
import org.w3c.dom.*;
import com.ibm.xml.parser.*;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.utils.nitree.*;

/**
 * This is the <code>PhysicalAccumulateOperator </code> that extends
 * the basic PhysicalOperator with the implementation of the Accumulate
 * operator.
 *
 * @version 1.0
 *
 * @author Kristin Tufte
 */

public class PhysicalAccumulateOperator extends PhysicalOperator {
    
    /*
     * Data members of the PhysicalAccumulateOperator Class
     */
    
    /*  
     * The array having information about blocking and non-blocking
     * streams.
     */

    private static final boolean[] blockingSourceStreams = { false, false };

    /* The merge tree  */
    private MergeTree mergeTree;
    private int mergeIndex;
    
    /* The object into which things are being accumulated */
    private NIDocument accumDoc;
    
    /* map table to use for accumDoc */
    private MapTable mapTable;

    /*
     * Methods of the PhysicalMergeOperator Class
     */ 

    /**
     * Constructor for PhysicalAccumulateOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * destination streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalAccumulateOperator (op logicalOperator,
				       Stream[] sourceStreams,
				       Stream[] destinationStreams,
				       Integer responsiveness) 
        {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Type cast the logical operator to a Accumulate operator
	//
	AccumulateOp logicalAccumulateOperator = 
	    (AccumulateOp) logicalOperator;

	mergeTree = logicalAccumulateOperator.getMergeTree();
	mergeIndex = logicalAccumulateOperator.getMergeIndex();
	/* for now, just create a new map table for the accumulate
	 * operator.  Ideally, we would have a MapTable per stream
	 * which might be shared by multiple operators, but since I
	 * don't like this MapTable stuff and since there is no good
	 * way to get a particular MapTable instance in here from
	 * the ExecutionScheduler (putting a MapTable in the logical
	 * operator seems too ugly), I will do this and punt on the
	 * problem
	 */
	mapTable = new MapTable(); 

	/* create an empty accumulator */
	createEmptyAccumulator();
    }
		     

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean blockingProcessSourceTupleElement (StreamTupleElement tupleElement,
							 int streamId) {

	/* get the fragment to be merged from the tuple, convert it
	 * to an element if necessary, then pass the work off to the merge tree
	 */
	try {
	    NIElement fragment = convertAttrToNIElement(tupleElement);
	    
	    // merge the fragment into the accumulated document
	    mergeTree.accumulate(accumDoc, fragment);
	    
	    return true;
	} catch (OpExecException e) {
	    return false; /* I think there's nothing better to do!! yeuch!!! */
	}
    }

    
    /**
     * This function removes the effects of the partial results in a given
     * source stream. This function over-rides the corresponding function
     * in the base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     * @return True if the operator is to proceed and false otherwise.
     */

    protected boolean removeEffectsOfPartialResult (int streamId) {

	/* delete the results accumulated so far?? - what
	 * about a combo of partial and final tuples coming in??
	 * I think I should skip this one for now since I'm
	 * aiming at an accumulate file implementation and
	 * I don't know what a partial result is in the context
	 * of an accumulate file
	 */ 
	/* what if I just create a new accumDoc??  note we can
	 * ignore multiple input stream issues since
	 * accumulate is a unary operator 
	 */
        createEmptyAccumulator();
        return true;
    }

    protected boolean getCurrentOutput (ResultTuples resultTuples, 
					boolean partial) {
	/* set the writeable bits in the accumulated tree so
	 * that all nodes will have to be copied to be changed,
	 * then put the accumulated tree into the resultTuples
	 */
	accumDoc.globalSetWriteableFalse();

	StreamTupleElement result = new StreamTupleElement(partial);
	result.appendAttribute(accumDoc.getDomDoc()); /* doc or elt? */

	resultTuples.add(result, 0);

	return true;
    }


    private NIElement convertAttrToNIElement(StreamTupleElement tupleElement) 
	throws OpExecException {
	Object attr = tupleElement.getAttribute(mergeIndex);

	Element domElt;
	Document domDoc;
	if(attr instanceof Element) {
	    domElt = (Element)attr;
	    domDoc = ((Element)attr).getOwnerDocument();
	} else if (attr instanceof Document) {
	    domElt = ((Document)attr).getDocumentElement();
	    domDoc = (Document)attr;
	} else {
	    throw new OpExecException("Invalid instance type");
	}

	NIDocument niDoc = NIDocument.getAssocNIDocument(domDoc, mapTable);
	return niDoc.getAssocNIElement(domElt);
    }

    private void createEmptyAccumulator()  {
	accumDoc = new NIDocument();
	Document domDoc = new TXDocument();
	accumDoc.initialize(mapTable, domDoc);
	return;
    }
}



