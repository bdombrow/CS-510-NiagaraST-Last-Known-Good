
/**********************************************************************
  $Id: PhysicalPartialOperator.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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

/**
 * This is the <code>PhysicalPartialOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Partial
 * operator that determines the depth to which GetPartial control
 * elements flow.
 *
 * @version 1.0
 */

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

public class PhysicalPartialOperator extends PhysicalOperator {
	
    /////////////////////////////////////////////////////
    //   Data members of the PhysicalSelectOperator Class
    /////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    
    ///////////////////////////////////////////////////
    //   Methods of the PhysicalSelectOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalPartialOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * destination streams, and responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalPartialOperator (op logicalOperator,
				    Stream[] sourceStreams,
				    Stream[] destinationStreams,
				    Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);
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

	// Just write the input element as a result
	//
	result.add(tupleElement, 0);

	// Operator can continue
	//
	return true;
    }


    /**
     * This function propagates a GetPartialResult control element from a
     * destination stream. This function over-rides the corresponding
     * function in the base class.
     *
     * @param controlElement The control element to be propagated
     * @param streamId The destination stream from which the control element
     *                 was read
     *
     * @return True if the operator is to proceed and false otherwise
     *
     * @exception java.lang.InterruptedException If the thread is interrupted
     *            during execution
     * @exception NullElementException If controlElement is null
     * @exception StreamPreviouslyClosedException If an attempt is made to
     *            propagate control elements to a previously closed stream
     */

    protected boolean propagateDestinationGetPartialResultElement(
					  StreamControlElement controlElement,
					  int streamId)
	throws java.lang.InterruptedException,
	       NullElementException,
	       StreamPreviouslyClosedException {

	// Send a synchronize Partial result control element to all open
	// destination streams
	//
	boolean proceed = putControlElementToDestinationStreams(
		            new StreamControlElement(
				 StreamControlElement.SynchronizePartialResult));

	// Return whether operator can continue or not
	//
	return proceed;
    }

}
