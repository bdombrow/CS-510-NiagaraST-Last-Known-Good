
/**********************************************************************
  $Id: PhysicalSelectOperator.java,v 1.7 2002/09/24 23:18:46 ptucker Exp $


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

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This is the <code>PhysicalSelectOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Select
 * operator.
 *
 * @version 1.0
 */
 
public class PhysicalSelectOperator extends PhysicalOperator {
	
    /////////////////////////////////////////////////////
    //   Data members of the PhysicalSelectOperator Class
    /////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The is the predicate to apply to the tuples
    //
    private PredicateEvaluator predEval;
    

    ///////////////////////////////////////////////////
    //   Methods of the PhysicalSelectOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalSelectOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * sink streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param sinkStreams The Sink Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     * @param predicate The predicate evaluated in the select operator
     */
     
    public PhysicalSelectOperator (op logicalOperator,
                                   SourceTupleStream[] sourceStreams,
                                   SinkTupleStream[] sinkStreams,
                                   Integer responsiveness) {
		
	// Call the appropriate constructor of the super class
	super(sourceStreams, sinkStreams, 
	      blockingSourceStreams, responsiveness);
	
	// Type cast logical operator to a select operator
	selectOp logicalSelectOperator = (selectOp) logicalOperator;
	
	predEval =  new PredicateEvaluator(logicalSelectOperator.
					   getPredicate());
	
	// XXX hack
	clear = ((selectOp) logicalOperator).getClear();
    }
		     
    // XXX hack
    boolean[] clear;

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
			     StreamTupleElement inputTuple, int streamId)
	throws ShutdownException, InterruptedException {
	// Evaluate the predicate on the desired attribute of the tuple
	if (predEval.eval(inputTuple)) {
	    /*
	    // If the predicate is satisfied, add the tuple to the result
	    // XXX hack
	    StreamTupleElement newTuple = 
		new StreamTupleElement(inputTuple.isPartial());
	    System.out.println("clear.length: " + clear.length);
	    for (int i = 0; i < clear.length; i++) {
		if (clear[i] || i == 0) { // XXX hack on hack. doc is not variable
		    System.out.println("\tappending null");
		    newTuple.appendAttribute(null);
		} else {
		    System.out.println("\tappending attribute " + i);
		    newTuple.appendAttribute(inputTuple.getAttribute(i));
		}
	    }
	    putTuple(newTuple, 0);
	    */
	    putTuple(inputTuple, 0);
	} else {
	    return;
	}
    }
    
    /**
     * This function processes a punctuation element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * Punctuations can simply be sent to the next operator from Select
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void processPunctuation(StreamPunctuationElement inputTuple,
				      int streamId)
	throws ShutdownException, InterruptedException {
	if (inputTuple.isPunctuation()) {
	    putTuple(inputTuple, streamId);
	}
    }

    public boolean isStateful() {
	return false;
    }
}



