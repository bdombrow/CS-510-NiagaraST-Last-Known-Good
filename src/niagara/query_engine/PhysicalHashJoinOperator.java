
/**********************************************************************
  $Id: PhysicalHashJoinOperator.java,v 1.5 2002/04/29 19:51:23 tufte Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


package niagara.query_engine;

import java.util.Vector;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This is the <code>PhysicalHashJoinOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Hash
 * Join operator.
 *
 * @version 1.0
 *
 */

public class PhysicalHashJoinOperator extends PhysicalOperator {
	
    //////////////////////////////////////////////////////
    // Data members of the PhysicalHashJoinOperator Class
    //////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false, false };

    private PredicateEvaluator predEval;
    private Hasher[] hashers;

    // The array of hash tables of partial tuple elements that are read from the
    // source streams. The index of the array corresponds to the index of the
    // stream from which the tuples were read.
    //
    DuplicateHashtable[] partialSourceTuples;

    // The array of hash tables of final tuple elements that are read from the
    // source streams. The index of the array corresponds to the index of the
    // stream from which the tuples were read.
    //
    DuplicateHashtable[] finalSourceTuples;
    
    ///////////////////////////////////////////////////
    // Methods of the PhysicalHashJoinOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalHashJoinOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * sink streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param sinkStreams The Sink Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalHashJoinOperator (op logicalOperator,
				     SourceTupleStream[] sourceStreams,
				     SinkTupleStream[] sinkStreams,
				     Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      sinkStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Type cast the logical operator to a join operator
	//
	joinOp logicalJoinOperator = (joinOp) logicalOperator;

	predEval = new PredicateEvaluator(logicalJoinOperator.getPredicate());

        hashers = new Hasher[2];
        hashers[0] = new Hasher(logicalJoinOperator.getLeftEqJoinAttr());
        hashers[1] = new Hasher(logicalJoinOperator.getRightEqJoinAttr());

	// Initialize the array of hash tables of partial source tuples - there are
	// two input stream, so the array is of size 2
	//
	partialSourceTuples = new DuplicateHashtable[2];

	partialSourceTuples[0] = new DuplicateHashtable();
	partialSourceTuples[1] = new DuplicateHashtable();

	// Initialize the array of hash tables of final source tuples - there are
	// two input stream, so the array is of size 2
	//
	finalSourceTuples = new DuplicateHashtable[2];

	finalSourceTuples[0] = new DuplicateHashtable();
	finalSourceTuples[1] = new DuplicateHashtable();

    }
		     

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */

    protected void nonblockingProcessSourceTupleElement (
				     StreamTupleElement tupleElement,
				     int streamId) 
	throws ShutdownException, InterruptedException {
	// Get the hash code corresponding to the tuple element
	//
	String hashKey = hashers[streamId].hashKey(tupleElement);

	// First add the tuple element to the appropriate hash table
	//
	if (tupleElement.isPartial()) {
	    partialSourceTuples[streamId].put(hashKey, tupleElement);
	}
	else {
	    finalSourceTuples[streamId].put(hashKey, tupleElement);
	}
	
	// Determine the id of the other stream
	//
	int otherStreamId = 1 - streamId;
	
	// Now loop over all the partial elements of the other source 
	// and evaluate the predicate and construct a result tuple if 
	// the predicate is satisfied
	constructJoinResult(tupleElement, streamId, hashKey,
			    partialSourceTuples[otherStreamId]);
	
	// Now loop over all the final elements of the other source 
	// and evaluate the predicate and construct a result tuple if 
	// the predicate is satisfied
	constructJoinResult(tupleElement, streamId, hashKey,
			    finalSourceTuples[otherStreamId]);
    }


    /**
     * This function removes the effects of the partial results in a given
     * source stream. This function over-rides the corresponding function
     * in the base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void removeEffectsOfPartialResult (int streamId) {

	// Clear the list of tuples in the appropriate stream
	partialSourceTuples[streamId].clear();
    }


    /**
     * This function constructs a join result based on joining with tupleElement
     * from the stream with id streamId with the hash table of tuples of the other
     * stream represented by the hash table otherStreamTuples. The join results are
     * returned in result
     *
     * @param tupleElement The tuple to be joined with tuples in other stream
     * @param streamId The stream id of tupleElement
     * @param hashCode The join hash code
     * @param otherStreamTuples The tuples to be joined with tupleElement
     */

    private void constructJoinResult (StreamTupleElement tupleElement,
				      int streamId,
				      String hashKey,
				      DuplicateHashtable otherStreamTuples) 
	throws ShutdownException, InterruptedException{

	// Get the list of tuple elements having the same hash code in
	// otherStreamTuples
	//
	Vector sourceTuples = otherStreamTuples.get(hashKey);

	// Loop over all the elements of the other source stream and
	// evaluate the predicate and construct a result tuple if the
	// predicate is satisfied
	//
	int numTuples = 0;

	if (sourceTuples != null) {
	    numTuples = sourceTuples.size();
	}

	for (int tup = 0; tup < numTuples; ++tup) {

	    // Get the appropriate tuple for the other source stream
	    //
	    StreamTupleElement otherTupleElement = 
		(StreamTupleElement) sourceTuples.get(tup);

	    // Make the right order for predicate evaluation
	    //
	    StreamTupleElement leftTuple;
	    StreamTupleElement rightTuple;

	    if (streamId == 0) {
		leftTuple = tupleElement;
		rightTuple = otherTupleElement;
	    }
	    else {
		leftTuple = otherTupleElement;
		rightTuple = tupleElement;
	    }

	    // Check whether the predicate is satisfied
	    //
	    if (predEval.eval(leftTuple, rightTuple)) {

		// Yes, it is satisfied - so create a result. The result is
		// potentially partial if either of the tuples is potentially
		// partial
		//
		StreamTupleElement resultTuple = 
		    new StreamTupleElement(leftTuple.isPartial() ||
					   rightTuple.isPartial(),
					   leftTuple.size() + rightTuple.size());

		resultTuple.appendAttributes(leftTuple);
		resultTuple.appendAttributes(rightTuple);

		// Add the result to the output
		putTuple(resultTuple, 0);
	    }
	}
    }

    public boolean isStateful() {
	return true;
    }
}

