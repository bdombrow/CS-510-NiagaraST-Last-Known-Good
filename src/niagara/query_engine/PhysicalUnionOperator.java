
/**********************************************************************
  $Id: PhysicalUnionOperator.java,v 1.5 2002/09/24 23:18:46 ptucker Exp $


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
import java.util.ArrayList;
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

    ArrayList[] rgPunct;
    int[] rgnRemove;

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

	rgPunct = new ArrayList[sourceStreams.length];
	for (int i=0; i<rgPunct.length; i++)
	    rgPunct[i] = new ArrayList();

	rgnRemove = new int[sourceStreams.length];
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

    /**
     * This function handles punctuations for the given operator. For
     * Union, we have to make sure all inputs have reported equal
     * punctuation before outputting a punctuation.
     *
     * @param tuple The current input tuple to examine.
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void processPunctuation(StreamPunctuationElement tuple,
				      int streamId)
	throws ShutdownException, InterruptedException {
	
	boolean fAllMatch = true, fFound;

	//First, check to see if this punctuation matches a punctuation
	// from all other inputs
	for (int i=0; i < rgPunct.length && fAllMatch == true; i++) {
	    if (i != streamId) {
		fFound = false;
		for (int j=0; j < rgPunct[i].size() && fFound == false; j++) {
		    fFound =
			tuple.equals((StreamPunctuationElement) rgPunct[i].get(j));
		    if (fFound) rgnRemove[i] = j;
		}
		fAllMatch = fFound;
	    }
	}

	if (fAllMatch) {
	    //Output the punctuation
	    putTuple(tuple, 0);
	    //Remove the other punctuations, since they are no longer needed
	    for (int i=0; i < rgPunct.length; i++) {
		if (i != streamId)
		    rgPunct[i].remove(rgnRemove[i]);
	    }
	} else {
	    rgPunct[streamId].add(tuple);
	}
    }

    public boolean isStateful() {
	return false;
    }
}
