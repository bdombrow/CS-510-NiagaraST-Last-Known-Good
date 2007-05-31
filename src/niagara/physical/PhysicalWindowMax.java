/**********************************************************************
  $Id: PhysicalWindowMax.java,v 1.3 2007/05/31 03:36:23 jinli Exp $


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

import niagara.utils.Tuple;
import niagara.utils.BaseAttr;
import niagara.utils.LongAttr;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 * This is the <code>PhysicalCountOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * Count (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalWindowMax extends PhysicalWindowAggregate {

	/**
	 * This function updates the statistics with a value
	 *
	 * @param newValue The value by which the statistics are to be
	 *                 updated
	 */
	public void updateAggrResult (PhysicalWindowAggregate.AggrResult result, 
				  Object ungroupedResult) {
	// Increment the number of values
	// KT - is this correct??
	// code from old mrege results:
	//finalResult.updateStatistics(((Integer) ungroupedResult).intValue());
	result.count++;
	//double newValue = ((Double)((Vector)ungroupedResult).get(0)).doubleValue();
	double newValue = ((Double)ungroupedResult).doubleValue();
	if (newValue > result.doubleVal)  // doubleVal holds max
		result.doubleVal = newValue; 
	    
	}

    
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

	protected final Object constructUngroupedResult (Tuple 
							 tupleElement) {

	// First get the atomic values
	atomicValues.clear();
	ae.get(0).getAtomicValues(tupleElement, atomicValues);

	assert atomicValues.size() == 1 : "Must have exactly one atomic value";
	return new Double(((BaseAttr)atomicValues.get(0)).toASCII());
	}

	/**
	 * This function returns an empty result in case there are no groups
	 *
	 * @return The result when there are no groups. Returns null if no
	 *         result is to be constructed
	 */

	protected final BaseAttr constructEmptyResult () {
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

	protected final BaseAttr constructAggrResult (
				   PhysicalWindowAggregate.AggrResult partialResult,
				   PhysicalWindowAggregate.AggrResult finalResult) {
		   	
			// Create number of values and sum of values variables
			int numValues = 0;
			long max = 0;
			
			if (partialResult != null) {
				numValues += partialResult.count;
				if (partialResult.doubleVal > max)
				max = (long) partialResult.doubleVal; //Jenny:  Hack
			}
			if (finalResult != null) {
				numValues += finalResult.count;
				if (finalResult.doubleVal > max) 
				max = (long)finalResult.doubleVal; //Jenny: Hack
			}

			// If the number of values is 0, sum does not make sense
			if (numValues == 0) {
				assert false : "KT don't think returning null is ok";
				return null;
			}
			
			LongAttr resultElement = new LongAttr(max);
			return resultElement;
	}

	protected PhysicalWindowAggregate getInstance() {
	return new PhysicalWindowMax();
	}
}
