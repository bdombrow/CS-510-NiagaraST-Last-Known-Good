
/**********************************************************************
  $Id: PhysicalScan.java,v 1.2 2006/11/28 05:16:10 jinli Exp $


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


package niagara.physical;

import org.w3c.dom.*;

import niagara.logical.*;
import niagara.optimizer.colombia.LogicalOp;
import niagara.utils.*;
import niagara.xmlql_parser.*;

/**
 * The <code>PhysicalScanOperator</code> class is derived from the abstract class
 * <code>PhysicalOperator</code>. It implements a scan on a incoming tuple,
 * producing a new wider outgoing tuple.
 *
 * @version 1.0
 */

public class PhysicalScan extends UnoptimizablePhysicalOperator {
    // This is the array having information about blocking and non-blocking
    // streams
    private static final boolean[] blockingSourceStreams = { false };

    // The path expression to scan
    private regExp rExp;

    // The attribute on which the scan is to be performed
    private int scanField;

    private PathExprEvaluator pev;
    private NodeVector elementList;

    public void opInitFrom(LogicalOp logicalOperator) {
	// Type cast the logical operator to a scan operator
	ScanOp logicalScanOperator = (ScanOp) logicalOperator;
 
	// Sets the regular expression to scan for
	this.rExp = logicalScanOperator.getRegExpToScan();

	// Sets the field to scan on
	this.scanField = logicalScanOperator.getParent().getAttrId();

        pev = new PathExprEvaluator(rExp);
        elementList = new NodeVector();
    }

    // The required zero-argument constructor
    public PhysicalScan() {
        setBlockingSourceStreams(blockingSourceStreams);
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

    protected void processTuple (
					     Tuple inputTuple,
					     int streamId) 
	throws ShutdownException, InterruptedException {

	// Get the attribute to scan on
    assert inputTuple.getAttribute(scanField) instanceof XMLAttr:
    	"What?! It's not an XMLAttr? - Jenny";
	Node attribute = ((XMLAttr)inputTuple.getAttribute(scanField)).getNodeValue();
	
	// Get the nodes reachable using the path expression scanned
	pev.getMatches(attribute, elementList);	
		
	// Append all the nodes returned to the inputTuple and add these
	// to the result
	int numNodes = elementList.size();	

	for(int node = 0; node < numNodes; ++node) {
	    // Clone the input tuple to create an output tuple
	    // Append a reachable node to the output tuple
	    // and put the tuple in the output stream
	    Tuple outputTuple = 
		                   (Tuple) inputTuple.clone();
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
    protected void processPunctuation(Punctuation inputTuple,
				      int streamId)
	throws ShutdownException, InterruptedException {

	try {
	    // Get the attribute to scan on
	    Node attribute = ((XMLAttr)inputTuple.getAttribute(scanField)).getNodeValue();
	
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
		    Punctuation outputTuple = 
			(Punctuation) inputTuple.clone();
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
