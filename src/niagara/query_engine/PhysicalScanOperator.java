
/**********************************************************************
  $Id: PhysicalScanOperator.java,v 1.14 2002/10/24 02:53:06 vpapad Exp $


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
import java.util.ArrayList;
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

public class PhysicalScanOperator extends UnoptimizablePhysicalOperator {

    //////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalScanOperator class //
    //////////////////////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    private static final boolean[] blockingSourceStreams = { false };

    // The path expression to scan
    private regExp rExp;

    // The attribute on which the scan is to be performed
    private int scanField;

    private PathExprEvaluator pev;
    private NodeVector elementList;

    ///////////////////////////////////////////////////////////////////////////
    // These are the methods of the PhysicalScanOperatorClass                //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalScanOperator class that
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
     * @param rExp the path expression to evaluate for this scan
     * @param scanField the tuple field to begin the scan at
     */

    public PhysicalScanOperator (op logicalOperator,
				 SourceTupleStream[] sourceStreams,
				 SinkTupleStream[] sinkStreams,
				 Integer responsiveness) {

	// Call the constructor on the super class
	super(sourceStreams, sinkStreams, 
              blockingSourceStreams, responsiveness);

	// Type cast the logical operator to a scan operator
	scanOp logicalScanOperator = (scanOp) logicalOperator;
 
	// Sets the regular expression to scan for
	this.rExp = logicalScanOperator.getRegExpToScan();

	// Sets the field to scan on
	this.scanField = logicalScanOperator.getParent().getAttrId();

        pev = new PathExprEvaluator(rExp);
        elementList = new NodeVector();
    }


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

	// Get the attribute to scan on
	Node attribute = inputTuple.getAttribute(scanField);
	
	// Get the nodes reachable using the path expression scanned
	pev.getMatches(attribute, elementList);	
		
	// Append all the nodes returned to the inputTuple and add these
	// to the result
	int numNodes = elementList.size();	

	for(int node = 0; node < numNodes; ++node) {
	    // Clone the input tuple to create an output tuple
	    // Append a reachable node to the output tuple
	    // and put the tuple in the output stream
	    StreamTupleElement outputTuple = 
		                   (StreamTupleElement) inputTuple.clone();
	    outputTuple.appendAttribute(elementList.get(node));
	    putTuple(outputTuple, 0);
	}
        elementList.clear();
    }    

    /**
     * This function processes a punctuation element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * Punctuations can simply be sent to the next operator from Scan
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void processPunctuation(StreamPunctuationElement inputTuple,
				      int streamId)
	throws ShutdownException, InterruptedException {

	try {
	    // Get the attribute to scan on
	    Node attribute = inputTuple.getAttribute(scanField);
	
	    // Get the nodes reachable using the path expression scanned
	    pev.getMatches(attribute, elementList);	
		
	    // Append all the nodes returned to the inputTuple and add these
	    // to the result
	    int numNodes = elementList.size();	

	    if (numNodes != 0) {
		for(int node = 0; node < numNodes; ++node) {
		    // Clone the input tuple to create an output tuple
		    // Append a reachable node to the output tuple
		    // and put the tuple in the output stream
		    StreamPunctuationElement outputTuple = 
			(StreamPunctuationElement) inputTuple.clone();
		    outputTuple.appendAttribute(elementList.get(node));
		    putTuple(outputTuple, 0);
		}
	    } else {
		//I still want the punctuation to live on, even if it doesn't
		// have the element we're scanning for.
		putTuple(inputTuple, streamId);
	    }
	    elementList.clear();
	} catch (java.lang.ArrayIndexOutOfBoundsException ex) {
	    //the scan field doesn't exist for this punctuation. We
	    // still want the tuple to live on.
	    putTuple(inputTuple, streamId);
	}
    }

    public boolean isStateful() {
	return false;
    }
}
