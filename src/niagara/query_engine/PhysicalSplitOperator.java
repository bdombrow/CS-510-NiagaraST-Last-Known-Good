
/**********************************************************************
  $Id: PhysicalSplitOperator.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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
import com.ibm.xml.parser.*;
import org.w3c.dom.*;

public class PhysicalSplitOperator extends PhysicalOperator {
	
    ////////////////////////////////////////////////////////
    // Data members of the PhysicalSplitOperator Class
    ////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The number of destination streams for the operator
    //
    int numDestinationStreams;

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
     * destination streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalSplitOperator (op logicalOperator,
				   Stream[] sourceStreams,
				   Stream[] destinationStreams,
				   Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

        outputTupleV = new Vector();
	// Get the number of destination streams
	//
	this.numDestinationStreams = Array.getLength(destinationStreams);

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
     * @param result The result is to be filled with tuples to be sent
     *               to destination streams
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected boolean nonblockingProcessSourceTupleElement (
						 StreamTupleElement tupleElement,
						 int streamId,
						 ResultTuples result) {
	
	outputTupleV.addElement(tupleElement);

	//the following code to exetract the destFileName is moved to 
	//data manager.

	/*
	// Copy the input tuple to corresponding destination streams
	//
        // First get the node of destFileName
        //
        // System.err.println("Split Now In EXECUTION " + tupleElement.size());
        Node node = (Element)tupleElement.getAttribute(tupleElement.size()-1);

        // Now get its first child
        //
        //for(int i=0; i<tupleElement.size(); i++) {
        // System.err.println("TupleElement At " + i + " " + 
        //        ((Node)tupleElement.getAttribute(i)).getNodeName());
        //}
        //System.err.println("Split debug print finished");
        Node firstChild = node.getFirstChild();

        // If such a child exists, then add its value to the result
        //
	String destFileName;
	Vector outputTuples = new Vector();
	
        if (firstChild != null) {
	    destFileName=firstChild.getNodeValue();
		
	    // need to modify the joined tuple to the original left input tuple
	    // element, since we don't want the hacked join and split affects the
	    // original logical plan
	    tupleElement.removeLastNAttributes(3);
            
	    DM.write(tupleElement,destFileName);
	    //result.add(tupleElement, dest);
	    // System.err.println("Happy!!!! Split got result!!" + 
            //        " put into destination " + dest);
	    // No problem - continue execution
	    //
	    return true;
	}
	else {
	    System.err.println("wrong constant table");
	    return false;
	}
	*/
	return true;
    }
}

