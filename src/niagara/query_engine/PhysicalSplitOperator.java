/**********************************************************************
  $Id: PhysicalSplitOperator.java,v 1.4 2002/10/24 01:20:18 vpapad Exp $


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

import java.lang.reflect.Array;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This is the <code>PhysicalSplitOperator</code> that extends
 * the basic PhysicalOperator. The Split operator exams the
 * contents of its input stream and output to corresponding output streams.
 *
 * @version 1.0
 *
 */ 
import java.util.*;
import org.w3c.dom.*;

public class PhysicalSplitOperator extends UnoptimizablePhysicalOperator {
	
    ////////////////////////////////////////////////////////
    // Data members of the PhysicalSplitOperator Class
    ////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The number of sink streams for the operator
    //
    int numSinkStreams;

    // The vector to store all the output tuples which will be 
    //write out when the operator is shutdown.
    Vector outputTupleV;

    // The destion of the operator
    String destFileName; 

    ///////////////////////////////////////////////////
    // Methods of the PhysicalSplitOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalSplitOperator class that
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
     
    public PhysicalSplitOperator (op logicalOperator,
				  SourceTupleStream[] sourceStreams,
				  SinkTupleStream[] sinkStreams,
				  Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      sinkStreams,
	      blockingSourceStreams,
	      responsiveness);

        outputTupleV = new Vector();
	// Get the number of sink streams
	//
	this.numSinkStreams = Array.getLength(sinkStreams);

	destFileName = ((splitOp)logicalOperator).getDestFileName();
    }
		     

    protected void shutdownTrigOp() {
        System.err.println("Shutdown called ");
	if (destFileName==null)
	    DM.storeTuples(outputTupleV); 
	else
	    DM.storeTuples(outputTupleV, destFileName); 
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
	throws ShutdownException {
	outputTupleV.addElement(tupleElement);
    }

    public boolean isStateful() {
	return false;
    }
}





