
/**********************************************************************
  $Id: PhysicalHeadOperator.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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
 * This is the <code>PhysicalHeadOperator</code> that extends
 * the basic PhysicalOperator. The Head operator is present at
 * the root of every query and responds to query engine administration
 * commands by shutting down the execution. Also, at the end of the
 * execution, it removes traces of the query from the system.
 *
 * @version 1.0
 *
 */
import niagara.utils.*;

public class PhysicalHeadOperator extends PhysicalOperator {
	
    /////////////////////////////////////////////////////
    //   Data members of the PhysicalHeadOperator Class
    /////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The information about query
    //
    QueryInfo queryInfo;
    

    ///////////////////////////////////////////////////
    //   Methods of the PhysicalSelectOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalSelectOperator class that
     * initializes it with the appropriate query info, source streams,
     * destination streams, and the responsiveness to control information.
     *
     * @param queryInfo The information about the query
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalHeadOperator (QueryInfo queryInfo,
				 Stream[] sourceStreams,
				 Stream[] destinationStreams,
				 Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Store the query information
	//
	this.queryInfo = queryInfo;
    }
		     

    /**
     * This function initializes the data structures for an operator.
     * This over-rides the corresponding operator in the base class.
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean initialize () {

	// Set this thread in the query info object
	//
	queryInfo.setHeadOperatorThread(Thread.currentThread());

	// Operator can continue
	//
	return true;
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

	// Just put the input to the output
	//
	result.add(tupleElement, 0);

	// No problem - continue execution
	//
	return true;
    }


    /**
     * This function cleans up after the operator. This over-rides the
     * corresponding function in the base class.
     */

    protected void cleanUp () {

	// Remove the query info object from the active query list
	//
	
	// Hack added so client server queries are not removed
	// from their respective connections activeQuery list
	//
	if(queryInfo.removeFromActiveQueries())
	    queryInfo.removeFromActiveQueryList();
    }
}
