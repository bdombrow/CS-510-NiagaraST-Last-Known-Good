
/**********************************************************************
  $Id: PhysicalDuplicateOperator.java,v 1.1 2000/05/30 21:03:26 tufte Exp $


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

import com.ibm.xml.parser.*;
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

    // The number of destination streams for the operator
    //
    int numDestinationStreams;
    

    ///////////////////////////////////////////////////
    // Methods of the PhysicalDuplicateOperator Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalDuplicateOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * destination streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalDuplicateOperator (op logicalOperator,
				   Stream[] sourceStreams,
				   Stream[] destinationStreams,
				   Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Get the number of destination streams
	//
	this.numDestinationStreams = Array.getLength(destinationStreams);
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

	// Copy the input tuple to all the destination streams
	//
        
        // System.err.println("Dup Op called.  Dup element to " + 
        //        numDestinationStreams + " Streams");
        // System.err.println("The elmenet is " + tupleElement);
        try {
            TXDocument doc = (TXDocument)tupleElement.getAttribute(0);
            String rootName = doc.getRootName();
            /*
            if(rootName==null) {
                System.err.println("Got you!, NULL Root of DOC");
            }
            */
        } catch (Exception e) {
            System.err.println("Non doc tupleElement.  Go on");
        }
    
	for (int dest = 0; dest < numDestinationStreams; ++dest) {
	    result.add(tupleElement, dest);
	}

	// No problem - continue execution
	//
	return true;
    }

}
