/**********************************************************************
  $Id: PhysicalDuplicateOperator.java,v 1.5 2002/10/24 03:10:52 vpapad Exp $


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

import niagara.optimizer.colombia.*;
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
    // No blocking input streams
    private static final boolean[] blockingSourceStreams = { false };

    // The number of sink streams for the operator
    int numSinkStreams;
    
    public PhysicalDuplicateOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void initFrom(LogicalOp logicalOperator) { }


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
	for (int dest = 0; dest < numSinkStreams; ++dest) {
	    putTuple(tupleElement, dest);
	}
    }

    public boolean isStateful() {
	return false;
    }
    
    /**
     * @see niagara.query_engine.PhysicalOperator#plugInStreams(SourceTupleStream[], SinkTupleStream[], boolean[], Integer)
     */
    public void plugInStreams(
        SourceTupleStream[] sourceStreams,
        SinkTupleStream[] sinkStreams,
        boolean[] blockingSourceStreams,
        Integer responsiveness) {
        super.plugInStreams(
            sourceStreams,
            sinkStreams,
            responsiveness);
            this.numSinkStreams = sinkStreams.length;
    }
}
