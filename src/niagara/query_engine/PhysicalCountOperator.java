
/**********************************************************************
  $Id: PhysicalCountOperator.java,v 1.2 2000/08/07 02:00:49 vpapad Exp $


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

import com.ibm.xml.parser.TXElement;
import com.ibm.xml.parser.TXText;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;


/**
 * This is the <code>PhysicalCountOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * Count (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalCountOperator extends PhysicalGroupOperator {

    ////////////////////////////////////////////////////////////////////
    // These are private nested classes used internally               //
    ////////////////////////////////////////////////////////////////////

    /**
     * Instances of this class store sufficient statistics for computing
     * the Count
     */
    
    private class CountingSufficientStatistics {

	//////////////////////////////////////////////////////////
	// These are the private members of the class           //
	//////////////////////////////////////////////////////////

	// The number of values
	//
	int numValues;


	//////////////////////////////////////////////////////////
	// These are the methods of the class                   //
	//////////////////////////////////////////////////////////

	/**
	 * This is the constructor that initializes Count sufficient
	 * statistics
	 */

	public CountingSufficientStatistics () {

	    // No values initially
	    //
	    this.numValues = 0;
	}


	/**
	 * This function updates the statistics with a value
	 *
	 * @param newValue The value by which the statistics are to be
	 *                 updated
	 */

	public void updateStatistics (int newValue) {
	    // Increment the number of values
	    //
	    ++numValues;
	}


	/**
	 * This function returns the number of values
	 *
	 * @return Returns the number of values in the statistics
	 */

	public int getNumberOfValues () {

	    // Return the number of values
	    //
	    return numValues;
	}
    }


    ////////////////////////////////////////////////////////////////////
    // These are the private variables of the class                   //
    ////////////////////////////////////////////////////////////////////

    // This is the aggregating attribute for the Count operator
    //
    schemaAttribute countingAttribute;


    ////////////////////////////////////////////////////////////////////
    // These are the methods of the class                             //
    ////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalCountOperator class that
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

    public PhysicalCountOperator(op logicalOperator,
				   Stream[] sourceStreams,
				   Stream[] destinationStreams,
				   Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(logicalOperator,
	      sourceStreams,
	      destinationStreams,
	      responsiveness);

	// Get the counting attribute of the Count logical operator
	//
	countingAttribute = ((CountOp) logicalOperator).getCountingAttribute();
    }


    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // Count operator (specializing the group operator)                  //
    /////////////////////////////////////////////////////////////////////////

    /**
     * This function is called to initialize a grouping operator for execution
     * by setting up relevant structures etc.
     */

    protected final void initializeForExecution () {
	// Nothing to do
	//
    }


    /**
     * This function constructs a ungrouped result from a tuple
     *
     * @param tupleElement The tuple to construct the ungrouped result from
     *
     * @return The constructed object; If no object is constructed, returns
     *         null
     */

    protected final Object constructUngroupedResult (StreamTupleElement tupleElement) {

	// First get the atomic values
	//
	Vector atomicValues = 
	    PredicateEvaluator.getAtomicValues(tupleElement, countingAttribute);

	// If there is not exactly one atomic value, skip
	//
	if (atomicValues.size() != 1) {
	    return null;
	}
	else {
	    return new Integer(1);
	}
    }


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

    protected final Object mergeResults (Object groupedResult,
					 Object ungroupedResult) {

	// Set up the final result - if the groupedResult is null, then
	// create holder for final result, else just use groupedResult
	//
	CountingSufficientStatistics finalResult = null;
	if (groupedResult == null) {
	    finalResult = new CountingSufficientStatistics();
	}
	else {
	    finalResult = (CountingSufficientStatistics) groupedResult;
	}

	// Add effects of ungrouped result (which is an Integer)
	//
	finalResult.updateStatistics(((Integer) ungroupedResult).intValue());

	// Return the grouped result
	//
	return finalResult;
    }


    /**
     * This function returns an empty result in case there are no groups
     *
     * @return The result when there are no groups. Returns null if no
     *         result is to be constructed
     */

    protected final Object constructEmptyResult () {
	// Create an Count result element
	TXElement resultElement = new TXElement("Count");

	// Add the text node as a child of the element node
	resultElement.appendChild(new TXText("0"));
	
	// Return the result element
	return resultElement;
    }


    /**
     * This function constructs a result from the grouped partial and final
     * results of a group. Both partial result and final result cannot be null
     *
     * @param partialResult The partial results of the group (this can be null)
     * @param finalResult The final results of the group (this can be null)
     *
     * @return A results merging partial and final results; If no such result,
     *         returns null
     */

    protected final Object constructResult (Object partialResult,
					    Object finalResult) {
	int numValues = 0;

	// If the partial result is not null, update with partial result stats
	//
	if (partialResult != null) {

	    // Type cast partial result to a Count sufficient statistics
	    //
	    CountingSufficientStatistics partialStats =
		(CountingSufficientStatistics) partialResult;

	    // Update number of values
	    //
	    numValues += partialStats.getNumberOfValues();
	}
	
	// If the final result is not null, update with final result stats
	//
	if (finalResult != null) {
	    // Type cast final result to a Count sufficient statistics
	    //
	    CountingSufficientStatistics finalStats =
		(CountingSufficientStatistics) finalResult;

	    // Update number of values
	    //
	    numValues += finalStats.getNumberOfValues();
	}

	// Create an Count result element
	//
	TXElement resultElement = new TXElement("Count");

	// Create a text node having the string representation of Count
	//
	TXText childElement = new TXText(Integer.toString(numValues));

	// Add the text node as a child of the element node
	//
	resultElement.appendChild(childElement);
	
	// Return the result element
	//
	return resultElement;
    }
}