/**********************************************************************
  $Id: PhysicalWindowCountOperator.java,v 1.2 2003/12/04 02:13:04 jinli Exp $


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

import niagara.utils.StreamTupleElement;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import java.util.*;

/**
 * This is the <code>PhysicalCountOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * Count (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalWindowCountOperator extends PhysicalWindowAggregateOperator {

	int totalCost = 0;
	/**
	 * This function updates the statistics with a value
	 *
	 * @param newValue The value by which the statistics are to be
	 *                 updated
	 */
	public void updateAggrResult (PhysicalWindowAggregateOperator.AggrResult result, 
				  Object ungroupedResult) {
	// Increment the number of values
	// KT - is this correct??
	// code from old mrege results:
	//finalResult.updateStatistics(((Integer) ungroupedResult).intValue());
	
	int size = ((Vector)ungroupedResult).size();
	if (size == 2) {
		result.cost = ((Long)((Vector)ungroupedResult).get(1)).longValue();
	} else {
		assert ((Integer)((Vector)ungroupedResult).get(0)).intValue() == 1 :
				"KT BAD BAD BAD";
		assert size == 1: "Jenny: StreamTupleElement!";
		result.count++;	
	}
	    
/*	assert ((Integer)ungroupedResult).intValue() == 1 :
		"KT BAD BAD BAD";
	result.count++;*/
	
	
	}


	////////////////////////////////////////////////////////////////////
	// These are the private variables of the class                   //
	////////////////////////////////////////////////////////////////////

	// This is the aggregating attribute for the Count operator
	//Attribute countingAttribute;
    
    
    
	/////////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific   //
	// Count operator (specializing the group operator)                  //
	/////////////////////////////////////////////////////////////////////////


	/**
	 * This function constructs a ungrouped result from a tuple
	 *
	 * @param tupleElement The tuple to construct the ungrouped result from
	 *
	 * @return The constructed object; If no object is constructed, returns
	 *         null
	 */

	protected final Object constructUngroupedResult (StreamTupleElement 
							 tupleElement) {

	// First get the atomic values
		atomicValues.clear();
		ae.getAtomicValues(tupleElement, atomicValues);

	Vector vect = new Vector();
	assert atomicValues.size() == 1 : "Must have exactly one atomic value";
	
	vect.add(new Integer(1));
	//if (Long.parseLong((String)atomicValues.get(0)) == -1)  // if it is the indicator that a window is to be closed;
	if(tupleElement.timestamp != 0)
	{		
		vect.add(new Long( tupleElement.timestamp));
	}
	//return new Integer(1);
	return vect;
	}

	/**
	 * This function returns an empty result in case there are no groups
	 *
	 * @return The result when there are no groups. Returns null if no
	 *         result is to be constructed
	 */

	protected final Node constructEmptyResult () {
	// Create an Count result element
	Element resultElement = doc.createElement("Count");

	// Add the text node as a child of the element node
	resultElement.appendChild(doc.createTextNode("0"));
	
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

	protected final Node constructAggrResult (
				   PhysicalWindowAggregateOperator.AggrResult partialResult,
		   PhysicalWindowAggregateOperator.AggrResult finalResult) {
	int numValues = 0;
	double timestamp = 0;

	if (partialResult != null) {
		numValues += partialResult.count;
		timestamp = System.currentTimeMillis() -  partialResult.cost;
	}
	
	if (finalResult != null) {
		numValues += finalResult.count;
		timestamp = System.currentTimeMillis() - finalResult.cost;
	}

	// Create an Count result element
	totalCost += timestamp;
	if (timestamp > 100000)
		timestamp = 0;
	Element resultElement = doc.createElement("Count");
	Text childElement = doc.createTextNode(Integer.toString(numValues) + "  accumulated cost: " + String.valueOf(totalCost) +  "  cost: " + String.valueOf(timestamp));
	resultElement.appendChild(childElement);
	
	return resultElement;
	}

	protected PhysicalWindowAggregateOperator getInstance() {
	return new PhysicalWindowCountOperator();
	}
}
