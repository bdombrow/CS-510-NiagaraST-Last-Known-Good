
/**********************************************************************
  $Id: PhysicalUnionOperator.java,v 1.4 2002/04/29 19:51:24 tufte Exp $


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
import java.util.Vector;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;


/**
 * <code>PhysicalUnionOperator</code> implements a union of a set 
 * of incoming streams;
 *
 * @author Vassilis Papadimos
 * @see PhysicalOperator
 */
public class PhysicalUnionOperator extends PhysicalOperator {

    //////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalUnionOperator class //
    //////////////////////////////////////////////////////////////////////////

    //private int count;

    /**
     * This is the constructor for the PhysicalUnionOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * sink streams, and responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param sinkStreams The Sink Streams associated with the
     *                           operator
     * @param blocking True if the operator is blocking and false if it is
     *                 non-blocking
     * @param responsiveness The responsiveness, in milliseconds, to control
     *                       messages
     */

    public PhysicalUnionOperator (op logicalOperator,
				  SourceTupleStream[] sourceStreams,
				  SinkTupleStream[] sinkStreams,
				  Integer responsiveness) {

	// Call the constructor on the super class
	//
	super(sourceStreams,
	      sinkStreams,
	      new boolean[sourceStreams.length],
	      responsiveness);

	//count = 0;
    }

    int counter = 0;

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */

    protected void nonblockingProcessSourceTupleElement (
						 StreamTupleElement inputTuple,
						 int streamId)
	throws ShutdownException, InterruptedException {
        counter++;
	putTuple(inputTuple, 0);
	}

    public boolean isStateful() {
	return false;
    }
}
