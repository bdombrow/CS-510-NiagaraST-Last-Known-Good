
/**********************************************************************
  $Id: PhysicalHashJoinOperator.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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

    // The predicate used for joining
    //
    private predicate joinPredicate;

    // The array of vectors of attributes in used for joining. The index of the
    // array corresponds to index of the input stream.
    //
    private Vector[] equalityJoinAttributes;

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
     * destination streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalHashJoinOperator (op logicalOperator,
				     Stream[] sourceStreams,
				     Stream[] destinationStreams,
				     Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Type cast the logical operator to a join operator
	//
	joinOp logicalJoinOperator = (joinOp) logicalOperator;

	// Set the predicate for evaluating the select
	//
	this.joinPredicate = logicalJoinOperator.getPredicate();

	// Initialize the array of equality join attributes - there are two
	// input streams, so the size of the array is 2
	//
	equalityJoinAttributes = new Vector[2];

	equalityJoinAttributes[0] = logicalJoinOperator.getLeftEqJoinAttr();
	equalityJoinAttributes[1] = logicalJoinOperator.getRightEqJoinAttr();

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
     * @param result The result is to be filled with tuples to be sent
     *               to destination streams
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean nonblockingProcessSourceTupleElement (
						 StreamTupleElement tupleElement,
						 int streamId,
						 ResultTuples result) {

	// Get the hash code corresponding to the tuple element
	//
	String hashCode = PredicateEvaluator.hashCode(tupleElement,
						      equalityJoinAttributes[streamId]);

	// Proceed only if this is a valid hash code
	//
	if (hashCode == null) {

	    // Nothing to do with invalid hash code; but operator can continue
	    //
	    System.err.println("Tuple with Invalid Hash Code");
	    return true;
	}
	else {

	    // First add the tuple element to the appropriate hash table
	    //
	    if (tupleElement.isPartial()) {
		partialSourceTuples[streamId].put(hashCode, tupleElement);
	    }
	    else {
		finalSourceTuples[streamId].put(hashCode, tupleElement);
	    }
	    
	    // Determine the id of the other stream
	    //
	    int otherStreamId = 1 - streamId;
	    
	    // Now loop over all the partial elements of the other source 
	    // and evaluate the predicate and construct a result tuple if the predicate
	    // is satisfied
	    //
	    boolean proceed = constructJoinResult(tupleElement,
						  streamId,
						  hashCode,
						  partialSourceTuples[otherStreamId],
						  result);
	    
	    if (!proceed) {
		
		// Ask operator to quit
		//
		return false;
	    }
	    
	    // Now loop over all the final elements of the other source 
	    // and evaluate the predicate and construct a result tuple if the predicate
	    // is satisfied
	    //
	    proceed = constructJoinResult(tupleElement,
					  streamId,
					  hashCode,
					  finalSourceTuples[otherStreamId],
					  result);
	    
	    // Continue execution or quit based on return value
	    //
	    return proceed;
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

	// Clear the list of tuples in the appropriate stream
	//
	partialSourceTuples[streamId].clear();

	// No problem - continue with operator
	//
	return true;
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
     * @param result The joined result tuples
     *
     * @return True if the operator is to proceed and false otherwise
     */

    private boolean constructJoinResult (StreamTupleElement tupleElement,
					 int streamId,
					 String hashCode,
					 DuplicateHashtable otherStreamTuples,
					 ResultTuples result) {

	// Get the list of tuple elements having the same hash code in
	// otherStreamTuples
	//
	Vector sourceTuples = otherStreamTuples.get(hashCode);

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
	    if (PredicateEvaluator.eval(leftTuple, rightTuple, joinPredicate)) {

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
		//
		result.add(resultTuple, 0);
	    }
	}

	// No problem - continue execution
	//
	return true;
    }
}


