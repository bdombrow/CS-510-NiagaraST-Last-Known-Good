
/**********************************************************************
  $Id: PhysicalGroupOperator.java,v 1.9 2002/04/19 20:49:15 tufte Exp $


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

import java.util.Vector;
import java.util.Hashtable;
import java.util.Collection;
import java.util.Iterator;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

import org.w3c.dom.*;

/**
 * This is the <code>PhysicalGroupOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the group
 * operator.
 *
 * @version 1.0
 *
 */

public abstract class PhysicalGroupOperator extends PhysicalOperator {

    /////////////////////////////////////////////////////////////////////////
    // These are private nested classes used within the operator           //
    /////////////////////////////////////////////////////////////////////////

    /**
     * This is the class that stores the information in an entry of the hash
     * table
     */
    private class HashEntry {
	
	/////////////////////////////////////////////////////////////////
	// These are the private members of the class                  //
	/////////////////////////////////////////////////////////////////

	// This is the object representing the final results
	//
	private Object finalResult;

	// This is the object representing the partial results
	//
	private Object partialResult;

	// This is the id of the currency of the partial results
	//
	private int partialResultId;

	// This is a representative tuple of the hash entry
	//
	StreamTupleElement representativeTuple;

	/////////////////////////////////////////////////////////////////
	// These are the methods of the class                          //
	/////////////////////////////////////////////////////////////////

	/**
	 * This is the constructor of a hash entry that initialize it with
	 * a representative tuple
	 *
	 * @param currPartialResultId The current id of partial results
	 * @param representativeTuple A representative tuple for this hash entry
	 */

	public HashEntry (int currPartialResultId,
			  StreamTupleElement representativeTuple) {

	    // Initialize the results to nothing
	    //
	    finalResult = null;
	    partialResult = null;

	    // Initialize the partial result id
	    //
	    this.partialResultId = currPartialResultId;

	    // Initialize the representative tuple
	    //
	    this.representativeTuple = representativeTuple;
	}

	/**
	 * This function returns the final result associated with this entry
	 *
	 * @return The final result
	 */

	public Object getFinalResult () {

	    // Return the final result
	    //
	    return finalResult;
	}


	/**
	 * This function sets the final result of this entry
	 *
	 * @param finalResult The final result to be set
	 */

	public void setFinalResult (Object finalResult) {

	    this.finalResult = finalResult;
	}


	/**
	 * This function returns the partial result associated with this entry
	 *
	 * @return The partial result
	 */

	public Object getPartialResult () {

	    // Return the partial result
	    //
	    return partialResult;
	}


	/**
	 * This function sets the partial result of this entry
	 *
	 * @param partialResult The partial result to be set
	 */

	public void setPartialResult (Object partialResult) {

	    this.partialResult = partialResult;
	}


	/**
	 * This function updates the partial result to make it consistent with the
	 * current partial result id
	 *
	 * @param currPartialResultId The current partial result id of the operator
	 */

	public void updatePartialResult (int currPartialResultId) {

	    // If the stored partial id is less than the current partial id, then
	    // clear the partial result and update the stored partial id
	    //
	    if (partialResultId < currPartialResultId) {

		// Clear the partial results
		//
		partialResult = null;

		// Update the stored partial result id
		//
		partialResultId = currPartialResultId;
	    }
	}


	/**
	 * This function returns the representative tuple associated with
	 * this hash entry
	 *
	 * @return The representative tuple associated with the hash entry
	 */

	public StreamTupleElement getRepresentativeTuple () {

	    // Return the representative tuple
	    //
	    return representativeTuple;
	}
    }


    /////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalGroupOperator     //
    // class                                                               //
    /////////////////////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { true };

    // The logical operator for grouping
    //
    private groupOp logicalGroupOperator;

    // The list of attributes to group by
    //
    private Vector groupAttributeList;

    private Hasher hasher;

    // This is the hash table for performing grouping efficiently
    //
    private Hashtable hashtable;

    // This is the current partial id of the operator used to discard previous
    // partial results
    //
    private int currPartialResultId;
    
    protected Document doc;

    ///////////////////////////////////////////////////////////////////////////
    // These are the methods of the PhysicalGroupOperator class          //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalGroupOperator class that
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

    public PhysicalGroupOperator(op logicalOperator,
				 Stream[] sourceStreams,
				 Stream[] destinationStreams,
				 Integer responsiveness) {


	// Call the constructor of the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Typecast to a group logical operator
	//
	logicalGroupOperator = (groupOp) logicalOperator;
    }
    

    /**
     * This function initializes the data structures for an operator.
     * This over-rides the corresponding function in the base class.
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected final boolean initialize () {

	// Get the grouping attributes
	//
	skolem grouping = logicalGroupOperator.getSkolemAttributes();

	groupAttributeList = grouping.getVarList();

        hasher = new Hasher(groupAttributeList);

	// Initialize the hash table
	//
	hashtable = new Hashtable();

	// Initialize the current partial id to 0
	//
	currPartialResultId = 0;

	// Ask subclasses to initialize
	//
	this.initializeForExecution();

	// The operator continues
	//
	return true;
    }


    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a blocking state. This over-rides the
     * corresponding function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected final boolean blockingProcessSourceTupleElement (
					 StreamTupleElement tupleElement,
					 int streamId) 
    throws OpExecException {

	// First get the hash code for the grouping attributes
	//
	String hashKey = hasher.hashKey(tupleElement);

	// If this is not a valid hash code, then nothing to do
	//	    
	// First construct ungrouped result
	//
	Object ungroupedResult = this.constructUngroupedResult(tupleElement);
	if(ungroupedResult == null) {
	    System.out.println("ungrouped result is null here too");
	}
	
	// Probe hash table to see whether result for this hashcode
	// already exist
	//
	
	HashEntry prevResult = (HashEntry) hashtable.get(hashKey);
	
	if (prevResult == null) {
	    
	    // If it does not have the result, just create new one
	    // with the current partial result id with the tupleElement
	    // as the representative tuple
	    //
	    prevResult = new HashEntry(currPartialResultId, tupleElement);
	    
	    // Add the entry to hash table
	    //
	    hashtable.put(hashKey, prevResult);
	}
	else {
	    
	    // It did have the result - update partial results
	    //
	    prevResult.updatePartialResult(currPartialResultId);
	}
	
	// Based on whether the tuple represents partial or final results
	// merge ungrouped result with previously grouped results
	//
	if (tupleElement.isPartial()) {
	    
	    // Merge the partial result so far with current ungrouped result
	    //
	    Object newPartialResult = 
		this.mergeResults(prevResult.getPartialResult(),
				  ungroupedResult);
	    
	    // Update the partial result
	    //
	    prevResult.setPartialResult(newPartialResult);
	}
	else {
	    
	    // Merge the final result so far with current ungrouped result
	    //
	    Object newFinalResult =
		this.mergeResults(prevResult.getFinalResult(),
				  ungroupedResult);
	    
	    // Update the final result
	    //
	    prevResult.setFinalResult(newFinalResult);
	}
    
	// The operator can continue
	//
	return true;
    }


    /**
     * This function returns the current output of the operator. This
     * function is invoked only when the operator is blocking. This
     * over-rides the corresponding function in the base class.
     *
     * @param resultTuples The output array list to be filled with the
     *                     results computed and the destination streams
     *                     to which they are to be sent.
     * @param partial If this function call is due to a request for a
     *                partial result
     *
     * @return True if the operator is to continue and false otherwise
     */

    protected final boolean getCurrentOutput (ResultTuples resultTuples, boolean partial) {
	// Get all the values in the hashtable
	//
	Collection values = hashtable.values();

	// Get an iterator to the values
	//
	Iterator iter = values.iterator();

	// If the iterator does not have any values, then call empty construct
	//
	if (!iter.hasNext()) {

	    // Get the result object
	    //
	    Node emptyResult = this.constructEmptyResult();

	    // If there is a non- empty result, then create tuple and add to
	    // result
	    //
	    if (emptyResult != null) {

		// Create tuple
		//
		StreamTupleElement tupleElement = 
		    createTuple(emptyResult,
				null,          // No representative tuple
				partial);

		// Add the tuple to the result
		//
		resultTuples.add(tupleElement, 0);
	    }

	    // Done and the operator can continue
	    //
	    return true;
	}

	// For each group, construct results
	//
	while (iter.hasNext()) {

	    // Get the next element in the hash table
	    //
	    HashEntry hashEntry = (HashEntry) iter.next();

	    // Update hash entry for partial results
	    //
	    hashEntry.updatePartialResult(currPartialResultId);

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
		resultTuples.add(tupleElement, 0);
	    }
	}

	// The operator can continue
	//
	return true;
    }


    /**
     * This function removes the effects of the partial results in a given
     * source stream. This over-rides the corresponding function in the
     * base class.
     *
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     * @return True if the operator is to proceed and false otherwise.
     */

    protected final boolean removeEffectsOfPartialResult (int streamId) {

	// Just increment the current partial id
	//
	++currPartialResultId;

	// The operator can continue
	//
	return true;
    }

    
    /**
     * This function creates a group tuple given the grouped result
     *
     * @param groupedResult The grouped result
     * @param representativeTuple The representative tuple for the group
     * @param partial Whether the tuple is a partial or final result
     *
     * @return A tuple with the grouped result
     */

    private StreamTupleElement createTuple (Node groupedResult,
					    StreamTupleElement representativeTuple,
					    boolean partial) {

	// Create a result tuple element tagged appropriately as
	// partial or final
	//
	StreamTupleElement tupleElement = new StreamTupleElement(partial);

	// NOTE: When grouping is implemented right at the logical operator tree
	// level, all that has to be done is to uncomment the following fragment
	// This ensures that the grouping attributes are part of the tuple result
	//
	
	// For each grouping attribute, add the corresponding element
	// to the result tuple from the representative tuple
	//
	int numGroupingAttributes = groupAttributeList.size();

	    for (int grp = 0; grp < numGroupingAttributes; ++grp) {
		// Get the group attribute
		int attributeId = 
		    ((schemaAttribute) groupAttributeList.elementAt(grp)).getAttrId();

		// Append the relevant attribute from the representative tuple
		// to the result
		if (representativeTuple != null) 
		    tupleElement.appendAttribute(representativeTuple.getAttribute(attributeId));
		else
		    tupleElement.appendAttribute(null);
	    }
	

	// Add the grouped result as the attribute
	//
	tupleElement.appendAttribute(groupedResult);


	// Return the result tuple
	//
	return tupleElement;
    }

    public void setResultDocument(Document doc) {
        this.doc = doc;
    }

    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // group operators                                                     //
    /////////////////////////////////////////////////////////////////////////

    /**
     * This function is called to initialize a grouping operator for execution
     * by setting up relevant structures etc.
     */

    protected abstract void initializeForExecution ();


    /**
     * This function constructs a ungrouped result from a tuple
     *
     * @param tupleElement The tuple to construct the ungrouped result from
     *
     * @return The constructed object; If no object is constructed, returns
     *         null
     */

    protected abstract Object constructUngroupedResult (StreamTupleElement tupleElement) throws OpExecException;


    /**
     * This function merges a grouped result with an ungrouped result
     *
     * @param groupedResult The grouped result that is to be modified (this can
     *                      be null)
     * @param ungroupedResult The ungrouped result that is to be grouped with
     *                        groupedResult (this can never be null)
     *
     * @return The new grouped result
     */

    protected abstract Object mergeResults (Object groupedResult,
					    Object ungroupedResult);


    /**
     * This function returns an empty result in case there are no groups
     *
     * @return The result when there are no groups. Returns null if no
     *         result is to be constructed
     */

    protected abstract Node constructEmptyResult ();


    /**
     * This function constructs a result from the grouped partial and final
     * results of a group. Both partial result and final result cannot be null.
     *
     * @param partialResult The partial results of the group (this can be null)
     * @param finalResult The final results of the group (this can be null)
     *
     * @return A results merging partial and final results; If no such result,
     *         returns null
     */

    protected abstract Node constructResult (Object partialResult,
					     Object finalResult);
}
