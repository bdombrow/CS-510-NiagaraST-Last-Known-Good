/**********************************************************************
  $Id: PhysicalNLJoinOperator.java,v 1.14 2003/07/09 04:59:35 tufte Exp $


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

import java.util.ArrayList;

import niagara.logical.And;
import niagara.optimizer.colombia.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;

/** Naive nested-loops join */
public class PhysicalNLJoinOperator extends PhysicalJoin {
    // This is the array having information about blocking and non-blocking
    // streams
    private static final boolean[] blockingSourceStreams = { false, false };

    private PredicateImpl predEval;

    // The array of lists of partial tuple elements that are read from the source
    // streams. The index of the array corresponds to the index of the stream
    // from which the tuples were read.
    ArrayList[] partialSourceTuples;

    // The array of lists of final tuple elements that are read from the source
    // streams. The index of the array corresponds to the index of the stream
    // from which the tuples were read.
    ArrayList[] finalSourceTuples;

    public PhysicalNLJoinOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void initJoin(joinOp join) {
        // In NL join, we hope that most tuples do not satisfy 
        // the equijoin predicates, so we check them first
        joinPredicate =
            And.conjunction(
                join.getEquiJoinPredicates().toPredicate(),
                join.getNonEquiJoinPredicate());

        predEval = joinPredicate.getImplementation();
	initExtensionJoin(join);
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

    protected void nonblockingProcessSourceTupleElement(
        StreamTupleElement tupleElement,
        int streamId)
        throws ShutdownException, InterruptedException {

        // Determine the id of the other stream
        int otherStreamId = 1 - streamId;
	boolean producedResult;

        // Now loop over all the partial elements of the other source 
        // and evaluate the predicate and construct a result tuple 
        // if the predicate is satsfied
        producedResult = constructJoinResult(tupleElement,
					     streamId,
					     partialSourceTuples[otherStreamId]);

	// Extension join note: Extension join indicates that we
	// expect an extension join with referential integrity 
	// meaning that tuples on the denoted extension join side
	// join only with one tuple from the other side, if I
	// join with a tuple, in partial, no need to check final
	// if I join with tuple from partial or final, no need
	// to insert in tuple list

        // Now loop over all the final elements of the other source 
        // and evaluate the predicate and construct a result tuple 
        // if the predicate is satisfied
	if(!extensionJoin[streamId] || !producedResult) {
	    producedResult = constructJoinResult(tupleElement,
						 streamId,
					      finalSourceTuples[otherStreamId]);
	}

        // First add the tuple element to the appropriate source stream
        //
	if(!extensionJoin[streamId] || !producedResult) {
	    if (tupleElement.isPartial()) {
		partialSourceTuples[streamId].add(tupleElement);
	    } else {
		finalSourceTuples[streamId].add(tupleElement);
	    }
	} 

    }

    /**
     * This function removes the effects of the partial results in a given
     * source stream. This function over-rides the corresponding function
     * in the base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     */

    protected void removeEffectsOfPartialResult(int streamId) {

        // Clear the list of tuples in the appropriate stream
        partialSourceTuples[streamId].clear();
    }

    /**
     * This function constructs a join result based on joining with tupleElement
     * from the stream with id streamId with the list of tuples of the other
     * stream represented by the list sourceTuples. The join results are
     * returned in result
     *
     * @param tupleElement The tuple to be joined with tuples in other stream
     * @param streamId The stream id of tupleElement
     * @param sourceTuples The tuples to be joined with tupleElement
     *
     */

    private boolean constructJoinResult(
        StreamTupleElement tupleElement,
        int streamId,
        ArrayList otherSourceTuples)
        throws ShutdownException, InterruptedException {

	boolean producedResult = false; 

        // Loop over all the elements of the other source stream and
        // evaluate the predicate and construct a result tuple if the
        // predicate is satisfied
        //

        for (int tup = 0; tup < otherSourceTuples.size(); ) {
            StreamTupleElement otherTupleElement =
                (StreamTupleElement) otherSourceTuples.get(tup);

            // Make the right order for predicate evaluation
            StreamTupleElement leftTuple;
            StreamTupleElement rightTuple;

            if (streamId == 0) {
                leftTuple = tupleElement;
                rightTuple = otherTupleElement;
            } else {
                leftTuple = otherTupleElement;
                rightTuple = tupleElement;
            }

	    boolean removed = false;
            // Check whether the predicate is satisfied
            if (predEval.evaluate(leftTuple, rightTuple)) {
                produceTuple(leftTuple, rightTuple);
		producedResult = true;
		if(extensionJoin[1-streamId]) {
		    // in extension join, know that tuple
		    // needs to be removed once it has joined
		    // we can return since we know will join with no more tuples
		    otherSourceTuples.remove(tup);
		    System.out.println("KT removing tuple " + (1-streamId));
		    removed = true;
		}
		if(extensionJoin[streamId])
		    return producedResult;
	    }
	    if(!removed)
		tup++;
        }
	return producedResult;
    }

    public boolean isStateful() {
        return true;
    }

    public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
        double leftCard = inputLogProp[0].getCardinality();
        double rightCard = inputLogProp[1].getCardinality();
        double outputCard = logProp.getCardinality();
        
        // we have to evaluate the predicate for every tuple combination
        Cost cost = predEval.getCost(catalog).times(leftCard * rightCard);
        cost.add(new Cost((leftCard + rightCard) + catalog.getDouble("tuple_reading_cost")));
        cost.add(new Cost(outputCard * constructTupleCost(catalog)));
        return cost;
    } 

    public PhysicalProperty[] inputReqdProp(
        PhysicalProperty PhysProp,
        LogicalProperty InputLogProp,
        int InputNo) {
        if (PhysProp.equals(PhysicalProperty.ANY) || InputNo == 1)
            return new PhysicalProperty[] {};
        else // require the same property for the left input
            return new PhysicalProperty[] {PhysProp};
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
     */
    public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
        // In terms of ordering, nested loops join has the properties
        // of its left input
        return input_phys_props[0].copy();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalNLJoinOperator op = new PhysicalNLJoinOperator();
        op.joinPredicate = joinPredicate;
        op.predEval = predEval;
	op.extensionJoin = extensionJoin;
        return op;
    }

    /**
     * @see niagara.query_engine.PhysicalOperator#opInitialize()
     */
    protected void opInitialize() {
        // Initialize the array of lists of partial source tuples
        partialSourceTuples = new ArrayList[] {new ArrayList(), new ArrayList()};

        // Initialize the array of lists of final source tuples 
        finalSourceTuples = new ArrayList[] {new ArrayList(), new ArrayList()};

        predEval.resolveVariables(inputTupleSchemas[0], 0);
        predEval.resolveVariables(inputTupleSchemas[1], 1);
    }
}
