
/**********************************************************************
  $Id: PhysicalTrigActionOperator.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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

import java.util.*;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.io.*;
import java.lang.reflect.Array;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This is the <code>PhysicalTrigActionOperator</code> that extends
 * the basic PhysicalOperator. 
 *
 * @version 1.0
 *
 */

public class PhysicalTrigActionOperator extends PhysicalOperator {
	
    ////////////////////////////////////////////////////////
    // Data members of the PhysicalDuplicateOperator Class
    ////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The number of destination streams for the operator
    //
    Vector action;
    String mailto;
    Document mailDoc;
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
     
    public PhysicalTrigActionOperator (op logicalOperator,
				   Stream[] sourceStreams,
				   Stream[] destinationStreams,
				   Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);
        action = ((trigActionOp)logicalOperator).getAction();
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

	// Copy the input tuple to the destination stream
	//
        // System.err.println("TrigAction Fired!");
	mailto = null;
        for(int i=0; i<action.size(); i++) {
	    String act = (String)action.elementAt(i);
	    if(act.substring(0, 6).equalsIgnoreCase("mailto")) {
		mailto = act.substring(8);
		break;
	    }
	    // System.err.println("Action: " + action.elementAt(i));
        }
	/*
        for(int i=0; i<tupleElement.size(); i++) {
            System.err.println("TupleElement At " + i + " " +
                    ((Node)tupleElement.getAttribute(i)).getNodeName());
        }
	*/
        // System.err.println("Debug triggaction done!");
	if(mailto==null)
	    result.add(tupleElement, 0); 
	else if(tupleElement.getAttribute(0) instanceof Document)
	    mailDoc = (Document)tupleElement.getAttribute(0);
	else {
	    if(mailDoc==null) { 
		mailDoc = new TXDocument();
		Element root = mailDoc.createElement("Results");
		mailDoc.appendChild(root);
	    }
	    Element tele = mailDoc.createElement("Result");
	    for(int i=0; i<tupleElement.size(); i++) {
		Node n = null;
		Object o = tupleElement.getAttribute(i);
		if(o instanceof String)
		    n = new TXText((String)o);
		else
		    n = (Node)o;
		tele.appendChild(n.cloneNode(true));
	    }
	    mailDoc.getDocumentElement().appendChild(tele);
	}
	return true;
    }
    protected void shutdownTrigOp() {
	if(mailto!=null) {
	    try {
		String fn = ".TrigMailto" + System.currentTimeMillis();
		File tmpF = new File(fn);
		FileWriter fw = new FileWriter(tmpF);
		((TXDocument)mailDoc).printWithFormat(fw);
		fw.flush();
		Runtime rt = Runtime.getRuntime();
		rt.exec("M " + mailto + " " + fn);
	    } catch (Exception ioe) {
		ioe.printStackTrace();
	    }
	}
    }
}


