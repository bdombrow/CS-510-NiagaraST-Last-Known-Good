
/**********************************************************************
  $Id: PhysicalWindowOperator.java,v 1.1 2003/02/05 21:17:50 jinli Exp $


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

import niagara.optimizer.colombia.*;

import java.util.Vector;
import java.util.Hashtable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Enumeration;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import java.util.ArrayList;
import niagara.logical.Variable;
import niagara.logical.NodeDomain;

import org.w3c.dom.*;

/**
 * This is the <code>PhysicalWindowOperator</code> that extends
 * the PhysicalGroupOperator with the implementation of the group
 * operator.
 *
 * @version 1.0
 *
 */

public abstract class PhysicalWindowOperator extends PhysicalGroupOperator {


    /////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalGroupOperator     //
    // class                                                               //
    /////////////////////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { true };

	//range of the sliding window
	//
    protected int range;
	
	//the sliding step of the sliding window
	//    
    protected int every;
    
    //window, together with streamIds, record the tuple element and Id 
    //for a tuple in the current sliding window
    //
    protected ArrayList window;

    //streamIds, together with window, record the tuple element and Id 
    //for a tuple in the current sliding window
    //    
    protected ArrayList streamIds;
    
    protected int count;

	//counter for the input tuples
	//
    protected int everyCount;

    ///////////////////////////////////////////////////////////////////////////
    // These are the methods of the PhysicalWindowOperator class          //
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
    public PhysicalWindowOperator() {


	// Call the constructor of the super class
	//
	super();

    }
/*    public PhysicalWindowOperator(op logicalOperator,
				 SourceTupleStream[] sourceStreams,
				 SinkTupleStream[] sinkStreams,
				 Integer responsiveness) {


	// Call the constructor of the super class
	//
	super(logicalOperator, sourceStreams, sinkStreams, responsiveness);

    }*/
    

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

    protected final void blockingProcessSourceTupleElement (
					 StreamTupleElement tupleElement,
					 int streamId) 
    throws UserErrorException, ShutdownException {

	if (range > 0) {	
		this.updateWindowStatus (tupleElement, streamId);
		windowResults();	
	}
	
    }

   
    /**
     * This function creates a result tuple with the window index
     *
     * @param groupedResult The grouped result
     * @param representativeTuple The representative tuple for the group
     * @param partial Whether the tuple is a partial or final result
     *
     * @return A tuple with the grouped result
     */

    protected StreamTupleElement createTuple (Node groupedResult,
					    StreamTupleElement representativeTuple,
					    boolean partial) {

	StreamTupleElement tupleElement =super.createTuple(groupedResult, 
								representativeTuple, partial);

	// Compute the window Index
	//
	int windowIndex = everyCount / every;

	int mod = everyCount % every;
	
	if (mod != 0) // Jenny - if the content of the last window contains less than #range tuples;
		windowIndex += 1;
			
	// Create a text node having the string representation of window index
	//
	//Text childElement = doc.createTextNode(Integer.toString(windowIndex));
	Element indexElement = doc.createElement("Index");

	// Create a text node having the string representation of max
	//
	Text childElement = doc.createTextNode(Integer.toString(windowIndex));

	// Add the text node as a child of the element node
	//
	indexElement.appendChild(childElement);	

	//tupleElement.appendAttribute(childElement);
	tupleElement.appendAttribute(indexElement);	

	// Return the result tuple
	//
	return tupleElement;
    }


    /**
     * This function maintains the current sliding window content
     *
     * @param tupleElement The current input tuple
     * @param streamId The ID of the stream where the current tuple comes from
     */
    
    protected void updateWindowStatus (StreamTupleElement tupleElement, int streamId) {

	    Integer id = new Integer(streamId);
    
    	if (count < range) {
    		window.add(count, tupleElement);
	    	streamIds.add(count, id);
    	} else {
    		window.remove(0);
    		streamIds.remove(0);
	    	window.add(range-1, tupleElement);
    		streamIds.add(range-1, id);
	    }
   
    	count++;
    	everyCount++;   
    }

    /**
     *This function constructs the output for each sliding window at each step, 
     * and maintains related data structure.
     *
     */
    
    protected void windowResults ()   
	throws UserErrorException, ShutdownException {
	
	int size = 0;
	try {
	if ((everyCount != 0) && (everyCount % every) == 0) {
		size = window.size();
		for (int i = 0; i < size; i++) 
			super.blockingProcessSourceTupleElement(((StreamTupleElement) window.get(i)), 
																		((Integer) streamIds.get(i)).intValue());

		flushWindowResults(false);
		clearHashtable();

	} 		
	} catch(InterruptedException e) {
		e.getMessage();
		throw new ShutdownException();
	}
	}
	
	
    /**
     *This function is for sliding window operators to reset the hashtable status for each window
     *
     */

    private final void clearHashtable() {
    super.hashtable.clear();
    }


    /**
     * This function returns the current output according to 
     * the the contennt of the sliding window
     *
     * @param partial If this function call is due to a request for a
     *                partial result
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected final void flushWindowResults(boolean partial) 
    throws InterruptedException, ShutdownException {
	// Get all the values in the hashtable and an iterator over the values
	Collection values = super.hashtable.values();
	Iterator iter = values.iterator();

	// If the iterator does not have any values, then call empty construct
	if (!iter.hasNext()) {
	    Node emptyResult = constructEmptyResult();

	    // If there is a non- empty result, then create tuple and add to
	    // result
	    if (emptyResult != null) {

		// Create tuple
		StreamTupleElement tupleElement = 
		    createTuple(emptyResult,
				null,          // No representative tuple
				partial);

		// Add the tuple to the result
		putTuple(tupleElement, 0);
	    }
	    return;
	}

	// For each group, construct results
	//
	while (iter.hasNext()) {

	    // Get the next element in the hash table
	    //
	    HashEntry hashEntry = (HashEntry) iter.next();

	    // Update hash entry for partial results
	    //
	    hashEntry.updatePartialResult(super.currPartialResultId);

	    // Get the result object if at least partial or final
	    // result is not null
	    //
	    Object partialResult = hashEntry.getPartialResult();
	    Object finalResult = hashEntry.getFinalResult();

	    Node resultNode = null;

	    if (partialResult != null || finalResult != null) {
		resultNode = this.constructResult(partialResult, finalResult);
	    }

	    // If there is a non- empty result, then create tuple and add to
	    // result
	    //
	    if (resultNode != null) {

		// Create tuple
		//
		StreamTupleElement tupleElement = 
		    createTuple(resultNode,
				hashEntry.getRepresentativeTuple(),
				partial);

		// Add the tuple to the result
		//
		putTuple(tupleElement, 0);
	    }
	}
	return;
    }
    
    /** Construct the output tuple schema, given the input schemas */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        // Default implementation: save input schema, 
        // use logical properties to construct output schema
        inputTupleSchemas = inputSchemas;
        outputTupleSchema = new TupleSchema();
        Attribute indexAttr;
        // By default, we assume attributes keep the order
        // they had in our logical property, which is the 
        // logical property of the first logical operator 
        // of the group. This will *not* be true in cases
        // where transformations produce equivalent logical
        // operators in the same group while changing the 
        // attribute order (e.g., joins).
        Attrs attrs = logProp.getAttrs();
        for (int i = 0; i < attrs.size(); i++) {
            outputTupleSchema.addMapping(attrs.get(i));
        }
 
 //       indexAttr = new Variable("index", NodeDomain.getDOMNode());       
 //       outputTupleSchema.addMapping(indexAttr);
        
        
    }
    
   
}
