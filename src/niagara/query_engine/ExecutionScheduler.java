/**********************************************************************
  $Id: ExecutionScheduler.java,v 1.18 2003/02/25 06:10:25 vpapad Exp $


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

import java.util.HashMap;
import niagara.data_manager.*;
import niagara.utils.*;
import niagara.ndom.*;

import niagara.connection_server.*;

/**
 * The class <code>ExecutionScheduler</code> schedules optimized queries
 * (represented by the optimized logical operator tree) for execution.
 *
 */

public class ExecutionScheduler {
    // The server we're running in
    private NiagraServer server;

    // This is the data manager that the physical operators interact with
    protected DataManager dataManager;

    // This is the operator queue in which all operators scheduled for
    // execution are to be put
    private PhysicalOperatorQueue opQueue;

    // This specifies the responsiveness of operators
    private static Integer responsiveness = new Integer(100);

    private boolean debug = false;

    /**
     * This is the constructor for the ExecutionScheduler that initializes
     * it with the operator queue in which it is to put operators scheduled
     * for execution.
     *
     * @param server The Niagara server we're running in

     * @param dataManager The data manager associated with the execution
     *                    scheduler to be contacted as necessary
     * @param opQueue The queue in which operators scheduled for execution
     *                are to be put
     */

    public ExecutionScheduler (NiagraServer server, DataManager dataManager,
			       PhysicalOperatorQueue opQueue) {

        this.server = server;
        this.opQueue = opQueue;
        this.dataManager = dataManager;
    }
     
    
    /**
     * This is the function that schedules all the operators in the optimized
     * logical operator tree for execution
     *
     * @param logicalOpTree The optimized logical operator tree
     * @param queryInfo Information about the query that is optimized
     *
     * @param outputStream The stream that returns result to user
     */

    public synchronized void executeOperators (SchedulablePlan optimizedTree,
					       QueryInfo queryInfo) 
	throws ShutdownException {

	// First create a Physical Head Operator to handle this query
	// in the system, only need to do this when top node
	// is SourceOp and can't function as head

	// where the top node of optimizedTree should put its output
	PageStream opTreeOutput;
	if(optimizedTree.isSource()) {
	    SinkTupleStream[] outputStreams = new SinkTupleStream[1];
	    outputStreams[0]= 
		new SinkTupleStream(queryInfo.getOutputPageStream());
	    
	    opTreeOutput = new PageStream("To: Head");
	    SourceTupleStream[] inputStreams = new SourceTupleStream[1];
	    inputStreams[0] = new SourceTupleStream(opTreeOutput);
	    
	    PhysicalHeadOperator headOperator = 
		new PhysicalHeadOperator(queryInfo,
					 inputStreams,
					 outputStreams,
					 responsiveness);
	    
	    // Put this operator in the execution queue
	    opQueue.putOperator(headOperator);
	} else {
	    opTreeOutput = queryInfo.getOutputPageStream();

	    // make the top operator function as a head op
	    optimizedTree.setIsHead(); 
	}
				     
	// Traverse the optimized tree and schedule the operators for
	// execution
	scheduleForExecution(optimizedTree, opTreeOutput,
			     new HashMap(), DOMFactory.newDocument(),
			     queryInfo);
    }

    public PageStream scheduleSubPlan(SchedulablePlan rootNode)
        throws ShutdownException {
        assert rootNode.isSchedulable();
        PageStream results = new PageStream("SubPlan");
        scheduleForExecution(
            rootNode,
            results,
            new HashMap(),
            DOMFactory.newDocument(),
            null);
        return results;
    }

    /**
     * This function schedules the DAG rooted at "rootLogicalNode" for
     * execution
     *
     * @param node            The root of the the logical tree to be
     *                        scheduled for execution
     * @param outputStream    The stream to which the output is to be sent
     * @param nodesScheduled  A hashtable containing all the logical plan nodes
     *                        that are already scheduled, since 
     *                        the plan is not necessarily a tree.
     * @param doc             The DOM document that will own any newly created XML nodes
     */
    private void scheduleForExecution (SchedulablePlan node,
				       PageStream outputStream,
				       HashMap nodesScheduled,
                                       Document doc, QueryInfo queryInfo) 
	throws ShutdownException {
	if (nodesScheduled.containsKey(node)) {
	    // This operator was already scheduled
	    // Just add outputStream to its output streams
            // XXX vpapad: Here we assume that source operators cannot
            // have multiple output streams
            PhysicalOperator physOp = (PhysicalOperator) nodesScheduled.get(node);
	    physOp.addSinkStream(new SinkTupleStream(outputStream));
	    if (physOp.isReady())
		opQueue.putOperator(physOp);
	    return;
	}

	// Handle the scan operators differently - these are operators
	// that can only appear at the very bottom of a query tree and
	// provide input for the query
	if(node.isSource()) {
	    // all source ops use SinkTupleStreams for output and these
	    // streams must reflect GET_PARTIALS 
	    SinkTupleStream sinkStream = 
		new SinkTupleStream(outputStream, true);
            node.processSource(sinkStream, dataManager);
            // XXX vpapad: we no longer have an operator to put here
            nodesScheduled.put(node, null);
	} else {
	    // regular operator node - create the output streams array
	    SinkTupleStream[] outputStreams = 
		new SinkTupleStream[node.getNumberOfOutputs()];
	    outputStreams[0] = new SinkTupleStream(outputStream);
	    
	    // Recurse over all children and create input streams array
	    int numInputs = node.getArity();
	    
	    SourceTupleStream[] inputStreams = 
		new SourceTupleStream[numInputs];
	    
	    for (int child = 0; child < numInputs; ++child) {
		// Create a new input stream
		PageStream inputPageStream = new PageStream("To: " +
						  node.getName());
		inputStreams[child] = new SourceTupleStream(inputPageStream);
		
		// Recurse on child
		scheduleForExecution(node.getInput(child),
				     inputPageStream,
				     nodesScheduled, doc, null);
	    }
	    
	    // Get the physical operator -
	    PhysicalOperator physicalOperator = node.getPhysicalOperator();
            physicalOperator.plugInStreams(inputStreams, outputStreams, responsiveness);

	    if(node.isHead()) {
		physicalOperator.setAsHead(queryInfo);
	    }
	    if (physicalOperator.isReady()) {
		// KT FIX - call this only if necessary
		physicalOperator.setResultDocument(doc);
		// Put the new created physical operator in the operator queue
		opQueue.putOperator(physicalOperator);
	    }
	    nodesScheduled.put(node, physicalOperator);
	}
    }
}

