/**********************************************************************
  $Id: PhysicalTrigActionOperator.java,v 1.6 2002/10/24 01:08:27 vpapad Exp $


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
import niagara.ndom.*;


// XXX vpapad: Document does not have printWithFormat - this code
// will only work with XML4J

/**
 * This is the <code>PhysicalTrigActionOperator</code> that extends
 * the basic PhysicalOperator. 
 *
 * @version 1.0
 *
 */

public class PhysicalTrigActionOperator extends UnoptimizablePhysicalOperator {
	
    ////////////////////////////////////////////////////////
    // Data members of the PhysicalDuplicateOperator Class
    ////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The number of sink streams for the operator
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
     * sink streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param sinkStreams The Sink Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
     
    public PhysicalTrigActionOperator (op logicalOperator,
				       SourceTupleStream[] sourceStreams,
				       SinkTupleStream[] sinkStreams,
				       Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(sourceStreams,
	      sinkStreams,
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
     *
     * @exception ShutdownException query shutdown by user or execution error
     */

    protected void nonblockingProcessSourceTupleElement (
				   	 StreamTupleElement tupleElement,
					 int streamId)
	throws ShutdownException, InterruptedException {
	// Copy the input tuple to the sink stream
	mailto = null;
        for(int i=0; i<action.size(); i++) {
	    String act = (String)action.elementAt(i);
	    if(act.substring(0, 6).equalsIgnoreCase("mailto")) {
		mailto = act.substring(8);
		break;
	    }
        }
	if(mailto==null)
	    putTuple(tupleElement, 0);
	else if(tupleElement.getAttribute(0) instanceof Document)
	    mailDoc = (Document)tupleElement.getAttribute(0);
	else {
	    if(mailDoc==null) { 
		mailDoc = DOMFactory.newDocument(); 
		Element root = mailDoc.createElement("Results");
		mailDoc.appendChild(root);
	    }
	    Element tele = mailDoc.createElement("Result");
	    for(int i=0; i<tupleElement.size(); i++) {
		Node n = null;
		Object o = tupleElement.getAttribute(i);
		if(o instanceof String)
		    n = mailDoc.createTextNode((String)o);
		else
		    n = (Node)o;
		tele.appendChild(n.cloneNode(true));
	    }
	    mailDoc.getDocumentElement().appendChild(tele);
	}
    }

    protected void shutdownTrigOp() {
	if(mailto!=null) {

	    if(mailDoc instanceof TXDocument) {
		// KT - works only with old version of XML4J -
		// printWithFormat is not part of the Document interface.
		try {
		    String fn = ".TrigMailto" + System.currentTimeMillis();
		    File tmpF = new File(fn);
		    FileWriter fw = new FileWriter(tmpF);
		
		    ((TXDocument)mailDoc).printWithFormat(fw);
		    fw.flush();
		    Runtime rt = Runtime.getRuntime();
		    rt.exec("M " + mailto + " " + fn);
		} catch (IOException ioe) {
		    System.err.println("shutdownTrigOp got IO exception : " +
				       ioe.getMessage());
		    ioe.printStackTrace();
		}
              } else {
		  throw new PEException("Trigger Send Mail did not complete - works only with old version of XML4J");
	    }
	}
    }

    public boolean isStateful() {
	return true;
    }
}


