
/**********************************************************************
  $Id: PhysicalWindowGroup.java,v 1.1 2003/12/24 01:49:02 vpapad Exp $


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

import java.util.ArrayList;

import niagara.logical.*;
import niagara.optimizer.colombia.*;
import niagara.utils.ShutdownException;
import niagara.utils.Tuple;
import org.w3c.dom.Node;


/**
 * This is the <code>PhysicalWindowGroup</code> that extends
 * the PhysicalGroupOperator with the implementation of the group
 * operator.
 *
 * @version 1.0
 *
 */

public abstract class PhysicalWindowGroup extends PhysicalGroup {


	/////////////////////////////////////////////////////////////////////////
	// These are the private data members of the PhysicalGroupOperator     //
	// class                                                               //
	/////////////////////////////////////////////////////////////////////////

	// This is the array having information about blocking and non-blocking
	// streams
	//
	private static final boolean[] blockingSourceStreams = { true };

	//streamIds, together with window, record the tuple element and Id 
	//for a tuple in the current sliding window
	//    
	protected ArrayList streamIds;
	protected Attribute windowAttr;
    
	///////////////////////////////////////////////////////////////////////////
	// These are the methods of the PhysicalWindowGroupOperator class          //
	///////////////////////////////////////////////////////////////////////////

	/**
	 * This is the constructor for the PhysicalWindowOperator class that
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
	public PhysicalWindowGroup() {

		// Call the constructor of the super class
		//
		super();
	}

	protected void localInitFrom(LogicalOp logicalOperator) {
		windowAttr = ((WindowGroup)logicalOperator).getWindowAttr();
	}

    

	/**
	 * This function processes a tuple element read from a source stream
	 * when the operator is in a blocking state. This over-rides the
	 * corresponding function in the base class.
	 *
	 * @param tupleElement The tuple element read from a source stream
	 * @param streamId The source stream from which the tuple was read
	 *
	 * @exception ShutdownException query shutdown by user or execution error
	 */

	protected final void blockingProcessTuple (
					 Tuple tupleElement,
					 int streamId) 
	throws ShutdownException {

		// First get the hash code for the grouping attributes
		
		int tupleSize = tupleElement.size();

		Node wid_from = tupleElement.getAttribute(tupleSize-2);
		Node wid_to = tupleElement.getAttribute(tupleSize-1);
		
		long from = Long.valueOf(wid_from.getFirstChild().getNodeValue()).intValue();
		long to = Long.valueOf(wid_to.getFirstChild().getNodeValue()).intValue();
		
		Tuple tmpTuple;
		tmpTuple = (Tuple) tupleElement.clone();
		//tmpTuple.deleteAttribute(1);
		Node wid;
		Node text;
		for (long i = from; i <= to; i++ ) {
			super.blockingProcessTuple(tmpTuple, streamId);			
			//wid = tmpTuple.getAttribute(tupleSize-2);
			//wid.setNodeValue(String.valueOf(i+1));
			wid = doc.createElement("wid_from");
			text = doc.createTextNode(String.valueOf(i+1));
			wid.appendChild(text);
			
			tupleElement.setAttribute(tupleSize-2, wid);					
		}
	
	}
}