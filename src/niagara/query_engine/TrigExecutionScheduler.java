
/**********************************************************************
  $Id: TrigExecutionScheduler.java,v 1.3 2002/03/26 23:52:32 tufte Exp $


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

import java.lang.reflect.Constructor;
import java.util.Vector;
import java.util.Hashtable;
import niagara.data_manager.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;


/**
 * The class <code>TrigExecutionScheduler</code> extends the 
 * class ExecutionScheduler and schedules group optimized queries
 * (represented by the optimized logical operator tree) for execution.
 *
 * @version 1.0
 *
 */

public class TrigExecutionScheduler extends ExecutionScheduler {

    ///////////////////////////////////////////////////
    // Nested classes private to TrigExecutionScheduler
    ///////////////////////////////////////////////////

    /**
     * This class stores the information for physical op and index
     * for allocating physical op for logical split and duplicate ops.
     */

    private class phyOpMapInfo {
     	
	public PhysicalOperator phyOp;
	public int currOutputStreamId;
	
	phyOpMapInfo(PhysicalOperator phyOp, int id) {
	    this.phyOp=phyOp;
	    currOutputStreamId=id;	    
	} 
    }
    
    //////////////////////////////////////////////////////////////////
    // These are private data members for the class                 //
    //////////////////////////////////////////////////////////////////
    // This is the operator queue in which all operators scheduled for
    // execution are to be put
    //
    private PhysicalOperatorQueue opQueue;

    // This specifies the responsiveness of operators
    //
    private static Integer responsiveness = new Integer(100);

    // Hashtable used for recording physical ops.
	//this hash table is used to record the information for 
	//allocating physical split and duplicate operator. For 
	//duplicate op, we will push the dupOp, physical op and 
	//the current 
	//output stream index into the hash table. Thus when meeting
	//the duplicator operator again, the right output stream will
	//will be used. For split op, since it has the information
	//of the relation between logical op and output channel,
	//we just store the split op , physical op and -1. 

    private Hashtable allocOps;
    
    //////////////////////////////////////////////////////////////////
    // These are the methods of the class                           //
    //////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the TrigExecutionScheduler that initializes
     * it with the operator queue in which it is to put operators scheduled
     * for execution.
     *
     * @param dataManager The data manager associated with the execution
     *                    scheduler to be contacted as necessary
     * @param opQueue The queue in which operators scheduled for execution
     *                are to be put
     */

    public TrigExecutionScheduler (DataManager dataManager,
			       PhysicalOperatorQueue opQueue) {

	super(null, dataManager, opQueue);	

	// Initialize the operator queue
	//
	this.opQueue = opQueue;
    }
     
    
    /**
     * This is the function that schedules all the operators in the group
     * optimized
     * logical operator trees for execution
     *
     * @param trigPlanRoots The roots of the group optimized logical operator trees
     * @param queryInfo Information about the query that is optimized
     *
     * @param outputStream The stream that returns result to user
     */

    public synchronized void cleanUpAlloc() {
        // System.err.println("cleaning up alloc table ... ");
        allocOps = null;
    }

    public synchronized void executeOperators (Vector trigPlanRoots,
					       Vector queryInfos) {

	// First create Physical Head Operators for each root to handle this query
	// in the system
	//
	for (int i=0; i<trigPlanRoots.size(); i++) {	    
	    Stream[] outputStreams = new Stream[1];
	    outputStreams[0] = ((QueryInfo)queryInfos.elementAt(i)).getOutputStream();

	    Stream[] inputStreams = new Stream[1];
	    inputStreams[0] = new Stream();

	    PhysicalHeadOperator headOperator = 
		new PhysicalHeadOperator((QueryInfo)queryInfos.elementAt(i),
					 inputStreams,
					 outputStreams,
					 responsiveness);
	    
	    // Put this operator in the execution queue
	    //
	    opQueue.putOperator(headOperator);
	    
	    // Traverse the optimized tree and schedule the operators for
	    // execution
	    //
	    scheduleForExecution(null,(logNode)trigPlanRoots.elementAt(i), inputStreams[0]);
	}
	
    }


    /**
     * This function schedules the tree rooted at "rootLogicalNode" for
     * execution
     *
     * @param parentLogicalNode the parent of the current logical node
     *
     * @param rootLogicalNode The root of the the logical tree to be
     *                        scheduled for execution
     * @param outputStream The stream to which the output is to be sent
     *
     * 
     */

    private void scheduleForExecution (logNode parentLogicalNode,
				       logNode rootLogicalNode,
				       Stream outputStream) {
        
	// Get the operator corresponding to the logical node
	op operator = rootLogicalNode.getOperator();
        
	// If this is a DTD Scan operator, then process accordingly
	if (operator instanceof dtdScanOp) {
	    processDTDScanOperator((dtdScanOp) operator, outputStream);
	} else {
	    // This is a regular operator node ... Create the output streams
	    // array
            // System.err.println("Scheduling a operator!");
	    Stream[] outputStreams;
	    
	    if ((operator instanceof dupOp) || (operator instanceof splitOp)) {
		//check whether this duplicate or split operator has been 
		//allocated for physical operator. If not, allocate a physical
		//operator, otherwise, not need to allocate again.
                // System.err.println("Now seeing a dup or split");
                if(allocOps==null) allocOps = new Hashtable();
                phyOpMapInfo pOpInfo = (phyOpMapInfo)allocOps.get(operator);
		
		if (pOpInfo==null) {
		    //this  logical operator has not been instantiated  
                    // System.err.println("It is a " + ((operator instanceof
                    //            dupOp) ? "Dup" : "Split")); 
		    int outputStreamNo;
		    if (operator instanceof dupOp) 
			outputStreamNo=((dupOp)operator).getNumDestinationStreams();
		    else
			outputStreamNo=((splitOp)operator).getNumDestinationStreams();
                    // System.err.println("outputStreamNo is " + outputStreamNo);
		    outputStreams = new Stream[outputStreamNo];		
		} else { 
		    //we don't need to go down, since the part under this
		    //split or dup op has been instantiated by previous
		    //round.
		    //find the right output stream corresponding to this parent
		    //and return it
                    // System.err.println("Seeing a old " + ((operator
                    //            instanceof dupOp) ? "Dup" : "Split"));
                    
		    PhysicalOperator phyOp=pOpInfo.phyOp;
		    int id;
		    if (operator instanceof dupOp) {
			id = ++(pOpInfo.currOutputStreamId);
			
		    } else {
			id = ((splitOp)operator).getCh(parentLogicalNode);
		    }

		    //set the output stream to its right position in 
		    //output stream array for split and duplicate operator
                    //System.err.println("Old split/dup: channel id " + id);
		    phyOp.setDestinationStream(id, outputStream);
		    return;		    
		}
	    } else { //normal operators other than Duplicate and Split	
		outputStreams = new Stream[1];
		outputStreams[0] = outputStream;
	    }
	    
	    // Recurse over all children and create input streams array
	    //
	    int numInputs = rootLogicalNode.numInputs();

	    Stream[] inputStreams = new Stream[numInputs];

	    for (int child = 0; child < numInputs; ++child) {

		// Create a new input stream
		//
		inputStreams[child] = new Stream();

		// Recurse on child
		//
		scheduleForExecution(rootLogicalNode, 
				     rootLogicalNode.input(child),
				     inputStreams[child]);
	    }

	    // Instantiate operator with input and output streams.
	    // The selected algorithm is instantiated
	    //
            Class physicalOperatorClass = operator.getSelectedAlgo();
            
            // System.err.println("ListOfAlgo Length ... " + operator.getListOfAlgo().length);
            
	    // If there is no selected algo, error
	    //
	    if (physicalOperatorClass == null) {
		System.err.println("Error: No algorithm selected during execution scheduling");
		return;
	    }

	    // System.out.println("Schedule Algorithm Name = " + physicalOperatorClass);

	    // Create a new instance of the class with the logical operator,
	    // input, output streams and responsiveness. First get the
	    // constructors
	    //
	    Constructor[] constructors = physicalOperatorClass.getConstructors();

	    // Now create an object array of the parameters for the
	    // constructor
	    //
	    Object[] parameters = new Object[4];

	    parameters[0] = operator;
	    parameters[1] = inputStreams;
	    parameters[2] = outputStreams;
	    parameters[3] = responsiveness;

	    // Create a new physical operator object
	    //
	    PhysicalOperator physicalOperator;

	    try {
		physicalOperator = 
		    (PhysicalOperator) constructors[0].newInstance(parameters);
	    }
	    catch (Exception e) {
		System.err.println("Error in Instantiating Physical Operator");
		return;
	    }
	    //split op which is the first time we traverse the tree
	    //and meet split and dup op.

	    //Set the output stream to its right position in 
	    //output stream array for split and duplicate operator
	    //Also allocate an entry in allocOps to prevent reallocatin
	    //physical operators for split and duplicate logical op.
	    
	    if ((operator instanceof dupOp) || (operator instanceof splitOp)) {	    
		int id;
		if (operator instanceof dupOp) {
		    id=0;					
		} else {
		    id=((splitOp)operator).getCh(parentLogicalNode);
		}	
                
               // System.err.println(operator + " : channel id is " + id);
               // System.err.println("Trying to find chanel for java Obj. "
               //                    + parentLogicalNode);
                physicalOperator.setDestinationStream(id, outputStream);
		phyOpMapInfo pOpInfo = new phyOpMapInfo(physicalOperator,0);
		allocOps.put(operator,pOpInfo);		    
	    } 

            // Put the new created physical operator in the operator queue
	    //
	    opQueue.putOperator(physicalOperator);
	}
    }
}
