
/**********************************************************************
  $Id: PhysicalUnionOperator.java,v 1.3 2001/08/08 21:27:57 tufte Exp $


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
     * destination streams, and responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param blocking True if the operator is blocking and false if it is
     *                 non-blocking
     * @param responsiveness The responsiveness, in milliseconds, to control
     *                       messages
     */

    public PhysicalUnionOperator (op logicalOperator,
				 Stream[] sourceStreams,
				 Stream[] destinationStreams,
				 Integer responsiveness) {

	// Call the constructor on the super class
	//
	super(sourceStreams,
	      destinationStreams,
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
     * @param result The result is to be filled with tuples to be sent
     *               to destination streams
     *
     * @return true if the operator is to continue and false otherwise
     */

    protected boolean nonblockingProcessSourceTupleElement (
						 StreamTupleElement inputTuple,
						 int streamId,
						 ResultTuples result) {
        counter++;
	    result.add(inputTuple, 0);
	    /*count++;
	    if(count%100 == 0) {
		System.out.print("U("+String.valueOf(count/100)+")");
	    }*/
	    return true;
	}
}
