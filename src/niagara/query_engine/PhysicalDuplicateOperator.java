
/**********************************************************************
  $Id: PhysicalDuplicateOperator.java,v 1.3 2002/04/29 19:51:23 tufte Exp $


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

import org.w3c.dom.*;
import java.lang.reflect.Array;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
/**
 * This is the <code>PhysicalDuplicateOperator</code> that extends
 * the basic PhysicalOperator. The Duplicate operator duplicates the
 * contents of its input stream to all its output streams.
 *
 * @version 1.0
 *
 */

public class PhysicalDuplicateOperator extends PhysicalOperator {
	
    ////////////////////////////////////////////////////////
    // Data members of the PhysicalDuplicateOperator Class
    ////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The number of sink streams for the operator
    //
    int numSinkStreams;
    

    ///////////////////////////////////////////////////
    // Methods of the PhysicalDuplicateOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalDuplicateOperator class that
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
     
    public PhysicalDuplicateOperator (op logicalOperator,
				      SourceTupleStream[] sourceStreams,
				      SinkTupleStream[] sinkStreams,
				      Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      sinkStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Get the number of sink streams
	//
	this.numSinkStreams = Array.getLength(sinkStreams);
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
	// Copy the input tuple to all the sink streams
        try {
            Document doc = (Document) tupleElement.getAttribute(0);
            String rootName = doc.getDocumentElement().getTagName();
        } catch (Exception e) {
	    throw new PEException("KT?? Non doc tupleElement. Go on");
        }
    
	for (int dest = 0; dest < numSinkStreams; ++dest) {
	    putTuple(tupleElement, dest);
	}
    }

    public boolean isStateful() {
	return false;
    }
    
}
