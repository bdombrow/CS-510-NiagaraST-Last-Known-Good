
/**********************************************************************
  $Id: PhysicalSumOperator.java,v 1.5 2002/04/08 19:03:09 vpapad Exp $


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

import java.util.ArrayList;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.ndom.*;


/**
 * This is the <code>PhysicalSumOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * Sum (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalSumOperator extends PhysicalGroupOperator {

    ////////////////////////////////////////////////////////////////////
    // These are private nested classes used internally               //
    ////////////////////////////////////////////////////////////////////

    /**
     * Instances of this class store sufficient statistics for computing
     * the sum
     */
    
    private class SummingSufficientStatistics {

	//////////////////////////////////////////////////////////
	// These are the private members of the class           //
	//////////////////////////////////////////////////////////

	// The number of values
	//
	int numValues;

	// The sum of the values
	//
	double sumOfValues;


	//////////////////////////////////////////////////////////
	// These are the methods of the class                   //
	//////////////////////////////////////////////////////////

	/**
	 * This is the constructor that initializes summing sufficient
	 * statistics
	 */

	public SummingSufficientStatistics () {

	    // No values initially
	    //
	    this.numValues = 0;

	    // Sum is initialized to 0
	    //
	    this.sumOfValues = 0;
	}


	/**
	 * This function updates the statistics with a value
	 *
	 * @param newValue The value by which the statistics are to be
	 *                 updated
	 */

	public void updateStatistics (double newValue) {
	    // Increment the number of values
	    //
	    ++numValues;

	    // Update the sum
	    //
	    sumOfValues += newValue;
	}


	/**
	 * This function returns the number of values
	 *
	 * @return Returns the number of values in the statistics
	 */

	public double getNumberOfValues () {

	    // Return the number of values
	    //
	    return numValues;
	}


	/**
	 * This function return the sum of the values in the sufficient
	 * statistics
	 *
	 * @return The sum of the values in the sufficient statistics
	 */

	public double getSumOfValues () {

	    // Return the sum of the values
	    //
	    return sumOfValues;
	}
    }


    ////////////////////////////////////////////////////////////////////
    // These are the private variables of the class                   //
    ////////////////////////////////////////////////////////////////////

    // This is the aggregating attribute for the sum operator
    //
    schemaAttribute summingAttribute;

    AtomicEvaluator ae;

    ArrayList atomicValues;

    ////////////////////////////////////////////////////////////////////
    // These are the methods of the class                             //
    ////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalSummingOperator class that
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

    public PhysicalSumOperator(op logicalOperator,
				   Stream[] sourceStreams,
				   Stream[] destinationStreams,
				   Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(logicalOperator,
	      sourceStreams,
	      destinationStreams,
	      responsiveness);

	// Get the summing attribute of the sum logical operator
	//
	summingAttribute = ((SumOp) logicalOperator).getSummingAttribute();
        ae = new AtomicEvaluator(summingAttribute);
        atomicValues = new ArrayList();
    }


    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // sum operator (specializing the group operator)                  //
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

    protected final Object constructUngroupedResult (StreamTupleElement 
						          tupleElement) 
	throws OpExecException {

	// First get the atomic values
        atomicValues.clear();
        ae.getAtomicValues(tupleElement, atomicValues);

	// If there is not exactly one atomic value, skip
	//
	if (atomicValues.size() != 1) {
	    System.out.println("Warning: not exactly one atomic value");
	    return null;
	}
	else {

	    // Get the string atomic value
	    //
	    String atomicValue = (String) atomicValues.get(0);

	    // Try to convert to double - if that fails, just return null
	    //
	    try {

		// Get the double value
		//
		Double doubleValue = new Double(atomicValue);

		// Return the double value
		//
		return doubleValue;
	    }
	    catch (java.lang.NumberFormatException e) {
		throw new OpExecException("Unable to convert atomicValue to double in PhysicalSumOperator");
	    }
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
	SummingSufficientStatistics finalResult = null;
	if (groupedResult == null) {
	    finalResult = new SummingSufficientStatistics();
	}
	else {
	    finalResult = (SummingSufficientStatistics) groupedResult;
	}

	// Add effects of ungrouped result (which is a Double)
	//
	if(finalResult == null) {
           System.out.println("aha, finalResult is null");
	} 
	if(ungroupedResult ==  null) {
           System.out.println("aha, ungroupedResult is null");
	}
	finalResult.updateStatistics(((Double) ungroupedResult).doubleValue());

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

	// Always return null
	//
	return null;
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

	// Create number of values and sum of values variables
	//
	int numValues = 0;
	double sumValues = 0;

	// If the partial result is not null, update with partial result stats
	//
	if (partialResult != null) {

	    // Type cast partial result to a summing sufficient statistics
	    //
	    SummingSufficientStatistics partialStats =
		(SummingSufficientStatistics) partialResult;

	    // Update number of values and the sum of values
	    //
	    numValues += partialStats.getNumberOfValues();
	    sumValues += partialStats.getSumOfValues();
	}
	
	// If the final result is not null, update with final result stats
	//
	if (finalResult != null) {

	    // Type cast final result to a summing sufficient statistics
	    //
	    SummingSufficientStatistics finalStats =
		(SummingSufficientStatistics) finalResult;

	    // Update number of values and sum of values
	    //
	    numValues += finalStats.getNumberOfValues();
	    sumValues += finalStats.getSumOfValues();
	}

	// If the number of values is 0, sum does not make sense
	//
	if (numValues == 0) {

	    return null;
	}

	// Compute the sum
	//
	double sum = sumValues;

	// Create a sum result element
	//
	Element resultElement = doc.createElement("Sum");

	// Create a text node having the string representation of sum
	//
	Text childElement = doc.createTextNode(Double.toString(sum));

	// Add the text node as a child of the element node
	//
	resultElement.appendChild(childElement);
	
	// Return the result element
	//
	return resultElement;
    }
}
