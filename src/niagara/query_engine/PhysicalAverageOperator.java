/**********************************************************************
  $Id: PhysicalAverageOperator.java,v 1.17 2003/07/09 04:59:35 tufte Exp $


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

import niagara.utils.ShutdownException;
import niagara.utils.StreamTupleElement;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;


/**
 * This is the <code>PhysicalAverageOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * average (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalAverageOperator extends PhysicalAggregateOperator { 

    /**
     * This function updates the statistics with a value
     *
     * @param newValue The value by which the statistics are to be
     *                 updated
     */
    public void updateAggrResult (PhysicalAggregateOperator.AggrResult result, 
				  Object ungroupedResult) {
	result.count++;
	result.doubleVal += ((Double) ungroupedResult).doubleValue(); // sum
    }
   

    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // average operator (specializing the group operator)                  //
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
						     tupleElement) 
	throws ShutdownException {
	return getDoubleValue(tupleElement);
    }

    /**
     * This function returns an empty result in case there are no groups
     *
     * @return The result when there are no groups. Returns null if no
     *         result is to be constructed
     */
    protected final Node constructEmptyResult () {
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
    protected final Node constructAggrResult (
		  PhysicalAggregateOperator.AggrResult partialResult,
		  PhysicalAggregateOperator.AggrResult finalResult) {

	// Create number of values and sum of values variables
	int numValues = 0;
	double sum = 0;

	if (partialResult != null) {
	    numValues += partialResult.count;
	    sum += partialResult.doubleVal;
	}
	if (finalResult != null) {
	    numValues += finalResult.count;
	    sum += finalResult.doubleVal;
	}

	// If the number of values is 0, average does not make sense
	if (numValues == 0) {
	    assert false : "KT don't think returning null is ok";
	    //return null;
	}

	// Create an average result element and return it
	Element resultElement = doc.createElement("Average");
	Text childElement = doc.createTextNode(Double.toString(sum/numValues));
	resultElement.appendChild(childElement);
	return resultElement;
    }

    protected PhysicalAggregateOperator getInstance() {
	return new PhysicalAverageOperator();
    }
}
