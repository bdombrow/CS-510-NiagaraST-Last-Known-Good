
/**********************************************************************
  $Id: PhysicalScanOperator.java,v 1.3 2000/08/09 23:53:59 tufte Exp $


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
import java.util.Vector;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * The <code>PhysicalScanOperator</code> class is derived from the abstract class
 * <code>PhysicalOperator</code>. It implements a scan on a incoming tuple,
 * producing a new wider outgoing tuple.
 *
 * @version 1.0
 */

public class PhysicalScanOperator extends PhysicalOperator {

    //////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalScanOperator class //
    //////////////////////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The path expression to scan
    //
    private regExp rExp;

    // The attribute on which the scan is to be performed
    //
    private int scanField;
    

    ///////////////////////////////////////////////////////////////////////////
    // These are the methods of the PhysicalScanOperatorClass                //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalScanOperator class that
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
     * @param rExp the path expression to evaluate for this scan
     * @param scanField the tuple field to begin the scan at
     */

    public PhysicalScanOperator (op logicalOperator,
				 Stream[] sourceStreams,
				 Stream[] destinationStreams,
				 Integer responsiveness) {

	// Call the constructor on the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Type cast the logical operator to a scan operator
	//
	scanOp logicalScanOperator = (scanOp) logicalOperator;
 
	// Sets the regular expression to scan for
	//
	this.rExp = logicalScanOperator.getRegExpToScan();

	// Sets the field to scan on
	//
	this.scanField = logicalScanOperator.getParent().getAttrId();

	
	}


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

	// Get the attribute to scan on
	//
	Object attribute = inputTuple.getAttribute(scanField);
	
	// Get the nodes reachable using the path expression scanned
	//
        
        if(attribute instanceof TXDocument) {
            // System.err.println( ((TXDocument)attribute).getText());
            String rootName = ((TXDocument)attribute).getRootName();
            if(rootName==null) {
                System.err.println("Got you!, NULL Root of DOC");
            }
        }
        else if(attribute instanceof TXElement) {
            // System.err.println( ((TXElement)attribute).getText());
        }
	//System.out.println("SCAN:Node scanned is " + ((Node)attribute).getNodeName());
		
	Vector elementList = PathExprEvaluator.getReachableNodes(attribute,this.rExp);
		
		

	// Append all the nodes returned to the inputTuple and add these
	// to the result
	//
	int numNodes = elementList.size();	

	for(int node = 0; node < numNodes; ++node) {

	    // Clone the input tuple to create an output tuple
	    //
	    StreamTupleElement outputTuple = 
		                   (StreamTupleElement) inputTuple.clone();

	    // Append a reachable node to the output tuple
	    //
	    outputTuple.appendAttribute(elementList.elementAt(node));

	    // Add the output tuple to the result
	    //
	    result.add(outputTuple, 0);
	}

	// No problem - continue execution
	//
	return true;
    }    
}
