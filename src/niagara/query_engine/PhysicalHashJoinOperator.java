/**********************************************************************
  $Id: PhysicalHashJoinOperator.java,v 1.11 2003/02/25 06:10:26 vpapad Exp $


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

import niagara.logical.*;
import niagara.optimizer.colombia.*;

import java.util.ArrayList;
import java.util.Enumeration;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;

/**
 * This is the <code>PhysicalHashJoinOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Hash
 * Join operator.
 *
 * @version 1.0
 *
 */

public class PhysicalHashJoinOperator extends PhysicalJoin {
    // No blocking input streams
    private static final boolean[] blockingSourceStreams = { false, false };

    // Optimization time structures
    /** Equijoin predicates */
    private EquiJoinPredicateList eqjoinPreds;
    /** Runtime implementation of joinPredicate */
    private PredicateImpl pred;

    private Hasher[] hashers;
    private String[] rgstPValues;
    private String[] rgstTValues;
    private ArrayList[] rgPunct = new ArrayList[2];

    // The array of hash tables of partial tuple elements that are read from the
    // source streams. The index of the array corresponds to the index of the
    // stream from which the tuples were read.
    DuplicateHashtable[] partialSourceTuples;

    // The array of hash tables of final tuple elements that are read from the
    // source streams. The index of the array corresponds to the index of the
    // stream from which the tuples were read.
    DuplicateHashtable[] finalSourceTuples;
    
    public PhysicalHashJoinOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void initJoin(joinOp join) {
        // In hash join, we hope that most tuples that hash the same
        // do indeed pass the equijoin predicates, so we put them last
        joinPredicate = And.conjunction(join.getNonEquiJoinPredicate(), 
                                        join.getEquiJoinPredicates().toPredicate());
        pred = joinPredicate.getImplementation();
        
        eqjoinPreds = join.getEquiJoinPredicates();
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
	String stPunctJoinKey = null;

	// Determine the id of the other stream
	int otherStreamId = 1 - streamId;
	boolean fMatch=false;
	
	// First add the tuple element to the appropriate hash table,
	//  but only if we haven't yet seen a punctuation from the
	//  other input that matches it
	for (int i=0; i<rgPunct[otherStreamId].size() && fMatch==false; i++) {
	    stPunctJoinKey =
		hashers[otherStreamId].hashKey
		((StreamPunctuationElement) rgPunct[otherStreamId].get(i));

	    if (stPunctJoinKey != null) {
		hashers[otherStreamId].getValuesFromKey(stPunctJoinKey,
							rgstPValues);
		hashers[streamId].getValuesFromKey(hashKey,
						   rgstTValues);
		boolean fMatchValue=true;

		for (int j=0; j<rgstPValues.length && fMatchValue; j++) {
		    fMatchValue =
			StreamPunctuationElement.matchValue(rgstPValues[j],
							    rgstTValues[j]);
		}
	    }
	}

	if (fMatch == false) {
	    if (tupleElement.isPartial()) {
		partialSourceTuples[streamId].put(hashKey, tupleElement);
	    } else {
		finalSourceTuples[streamId].put(hashKey, tupleElement);
	    }
	}
	
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

    private void constructJoinResult(StreamTupleElement tupleElement,
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
	    if (pred.evaluate(leftTuple, rightTuple))
                produceTuple(leftTuple, rightTuple);
	}
    }

    /**
     * This function handles punctuations for the given operator. The
     * join operator can use punctuations to purge some state.
     *
     * @param tuple The current input tuple to examine.
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void processPunctuation(StreamPunctuationElement tuple,
				      int streamId)
	throws ShutdownException, InterruptedException {

	try {
	    //first, add this to the appropriate list of punctuations
	    String stPunctKey = hashers[streamId].hashKey(tuple);
	    rgPunct[streamId].add(tuple);

	    //now, see if there are tuples to remove from the other hash table.
	    // check both the partial list and the final list
	    hashers[streamId].getValuesFromKey(stPunctKey, rgstPValues);
	    int otherStreamId = 1-streamId;

	    purgeHashTable(partialSourceTuples[otherStreamId],
			   otherStreamId, rgstPValues);
	    purgeHashTable(finalSourceTuples[otherStreamId],
			   otherStreamId, rgstPValues);
	} catch (java.lang.ArrayIndexOutOfBoundsException ex) {
	    //Not a punctuation for the join attribute. Ignore it.
	    ;
	}

	return;
    }

    private void purgeHashTable(DuplicateHashtable ht, int streamId,
				String[] rgstPunct) {
	Enumeration enKeys = ht.keys();
	while (enKeys.hasMoreElements()) {
	    String hashKey = (String) enKeys.nextElement();
	    hashers[streamId].getValuesFromKey(hashKey, rgstTValues);

	    boolean fMatch=true;
	    for (int i=0; i<rgstPunct.length && fMatch==true; i++) {
		fMatch = StreamPunctuationElement.matchValue(rgstPunct[i],
							     rgstTValues[i]);
	    }

	    if (fMatch)
		ht.remove(hashKey);
	}
    }

    public boolean isStateful() {
	return true;
    }
    

/**
 * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
 */
public Cost findLocalCost(
    ICatalog catalog,
    LogicalProperty[] InputLogProp) {
    float LeftCard = InputLogProp[0].getCardinality();
    float RightCard = InputLogProp[1].getCardinality();
    float OutputCard = logProp.getCardinality();

    double cost = 0;
    cost += (LeftCard + RightCard) * catalog.getDouble("tuple_reading_cost");
    cost += (LeftCard + RightCard) * catalog.getDouble("tuple_hashing_cost");
    cost += OutputCard * constructTupleCost(catalog);
    Cost c = new Cost(cost);
    // XXX vpapad: We must compute the predicate on all the tuple combinations
    // that pass the equality predicates we're hashing on; but how do we
    // compute that? We'll just assume that's the same as the tuples that
    // appear in the output (best case)
    c.add(pred.getCost(catalog).times(OutputCard));
    return c;
}


    /**
     * @see niagara.query_engine.PhysicalOperator#opInitialize()
     */
    protected void opInitialize() {
        Attrs leftAttrs = eqjoinPreds.getLeft();
        Attrs rightAttrs = eqjoinPreds.getRight();

        hashers = new Hasher[2];
        hashers[0] = new Hasher(leftAttrs);
        hashers[1] = new Hasher(rightAttrs);

        rgstPValues = new String[leftAttrs.size()];
        rgstTValues = new String[rightAttrs.size()];
        
        pred.resolveVariables(inputTupleSchemas[0], 0);
        pred.resolveVariables(inputTupleSchemas[1], 1);

        hashers[0].resolveVariables(inputTupleSchemas[0]);
        hashers[0].resolveVariables(inputTupleSchemas[1]);
        hashers[1].resolveVariables(inputTupleSchemas[0]);
        hashers[1].resolveVariables(inputTupleSchemas[1]);
        

        // Initialize the array of hash tables of partial source tuples - there are
        // two input stream, so the array is of size 2
        partialSourceTuples = new DuplicateHashtable[2];

        partialSourceTuples[0] = new DuplicateHashtable();
        partialSourceTuples[1] = new DuplicateHashtable();

        // Initialize the array of hash tables of final source tuples - there are
        // two input stream, so the array is of size 2
        finalSourceTuples = new DuplicateHashtable[2];

        finalSourceTuples[0] = new DuplicateHashtable();
        finalSourceTuples[1] = new DuplicateHashtable();

        //Initialize the punctuation lists
        rgPunct[0] = new ArrayList();
        rgPunct[1] = new ArrayList();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        PhysicalHashJoinOperator op = new PhysicalHashJoinOperator();
        op.pred = pred;
        op.joinPredicate = joinPredicate;
        op.eqjoinPreds = eqjoinPreds;
        return op;
    }
}

