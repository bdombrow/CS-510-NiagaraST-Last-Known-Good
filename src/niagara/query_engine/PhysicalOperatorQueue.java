
/**********************************************************************
  $Id: PhysicalOperatorQueue.java,v 1.1 2000/05/30 21:03:27 tufte Exp $


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

/////////////////////////////////////////////////////////////////////////
//
//   OperatorQueue.java:
//   NIAGRA Project
//   
//   Jayavel Shanmugasundaram
//
//   This is the Operator Queue class in which all operators scheduled for
//   execution are put. This class is synchronized for correct concurrent
//   access.
//
//
//   ---------------
//   Methods
//   ---------------
//
//   addOperator ()    // For adding an operator to the operator queue
//   getOperator ()    // Returns the next operator from the operator queue
//
//   ---------------
//   Data members
//   ---------------
//   Thread thread          // The Java Thread associated with the operator thread
//   OperatorQueue opQueue  // The Queue from which operators are removed
//
/////////////////////////////////////////////////////////////////////////

import niagara.utils.*;

public class PhysicalOperatorQueue {
	
    ///////////////////////////////////////////////////
    //   Data members of the PhysicalOperatorQueue Class
    ///////////////////////////////////////////////////

    // A synchronized queue for storing the operators
    //
    private SynchronizedQueue opQueue;

    
    ///////////////////////////////////////////////////
    //   Methods of the PhysicalOperatorQueue Class
    ///////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalOperatorQueue class that initializes
     * it to an empty queue.
     *
     * @param maxCapacity The maximum capacity of the operator queue
     */
     
    public PhysicalOperatorQueue (int maxCapacity) {

	// Call the constructor of the super class
	//
	super();

	// Create a synchronized queue to server as an operator queue
	//
	opQueue = new SynchronizedQueue (maxCapacity);

	// End of function
	//
	return;
    }
		     

    /**
     * This function adds an operator to the operator queue
     *
     * @param operator The operator to be added to the queue
     */
    public void putOperator (PhysicalOperator operator) {

	// Add the operator to the end of the queue
	//
	opQueue.put(operator, true);

	// End of function
	//
	return;
    }


    /**
     * This function gets an operator from the operator queue
     *
     * @return The operator at the head of the queu
     */

    public PhysicalOperator getOperator () {

	// Get the operator from the queue
	//
	PhysicalOperator tempOp = (PhysicalOperator) opQueue.get();

	// Return the operator
	//
	return tempOp;
    }

    public String toString()
    {
	return ("PhysicalOperatorQueue\n"+opQueue.toString());
    }
}
