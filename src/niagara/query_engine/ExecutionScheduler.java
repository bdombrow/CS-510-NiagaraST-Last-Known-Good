
/**********************************************************************
  $Id: ExecutionScheduler.java,v 1.3 2000/06/26 22:10:28 vpapad Exp $


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

/*
 * $RCSfile:
 * $Revision:
 * $Date:
 * $Author:
 */

package niagara.query_engine;

import java.lang.reflect.Constructor;
import java.util.Vector;
import java.util.Hashtable;
import niagara.data_manager.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;

/**
 * The class <code>ExecutionScheduler</code> schedules optimized queries
 * (represented by the optimized logical operator tree) for execution.
 *
 * @version 1.0
 *
 */

public class ExecutionScheduler {
    
    //////////////////////////////////////////////////////////////////
    // These are private data members for the class                 //
    //////////////////////////////////////////////////////////////////

    // This is the data manager that the physical operators interact with
    //
    private DataManager dataManager;

    // This is the operator queue in which all operators scheduled for
    // execution are to be put
    //
    private PhysicalOperatorQueue opQueue;

    // This specifies the capacity of streams
    //
    private static int streamCapacity = 2500;

    // This specifies the responsiveness of operators
    //
    private static Integer responsiveness = new Integer(100);


    //////////////////////////////////////////////////////////////////
    // These are the methods of the class                           //
    //////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the ExecutionScheduler that initializes
     * it with the operator queue in which it is to put operators scheduled
     * for execution.
     *
     * @param dataManager The data manager associated with the execution
     *                    scheduler to be contacted as necessary
     * @param opQueue The queue in which operators scheduled for execution
     *                are to be put
     */

    public ExecutionScheduler (DataManager dataManager,
			       PhysicalOperatorQueue opQueue) {

		// Initialize the operator queue
		//
		this.opQueue = opQueue;

		// Initialize the data manager
		//
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

    public synchronized void executeOperators (logNode optimizedTree,
											   QueryInfo queryInfo) {

		// First create a Physical Head Operator to handle this query
		// in the system
		//
		Stream[] outputStreams = new Stream[1];
		outputStreams[0] = queryInfo.getOutputStream();

		Stream[] inputStreams = new Stream[1];
		inputStreams[0] = new Stream(streamCapacity);

		PhysicalHeadOperator headOperator = 
			new PhysicalHeadOperator(queryInfo,
						 inputStreams,
						 outputStreams,
						 responsiveness);

		// Put this operator in the execution queue
		//
		opQueue.putOperator(headOperator);
				     
		// Traverse the optimized tree and schedule the operators for
		// execution
		//
		System.err.println("ES:: optimizedTree is: ");
		optimizedTree.dump();
		scheduleForExecution(optimizedTree, inputStreams[0]);
    }


    /**
     * This function schedules the tree rooted at "rootLogicalNode" for
     * execution
     *
     * @param rootLogicalNode The root of the the logical tree to be
     *                        scheduled for execution
     * @param outputStream The stream to which the output is to be sent
     */

    private void scheduleForExecution (logNode rootLogicalNode,
									   Stream outputStream) {

		// Get the operator corresponding to the logical node
		//
		op operator = rootLogicalNode.getOperator();

		// If this is a DTD Scan operator, then process accordingly
		//
		if (operator instanceof dtdScanOp) {

			processDTDScanOperator((dtdScanOp) operator, outputStream);
		} else if (operator instanceof FirehoseScanOp) {
		    processFirehoseScanOperator((FirehoseScanOp) operator, outputStream);
		}
		else {

			// This is a regular operator node ... Create the output streams
			// array
			//
			Stream[] outputStreams = new Stream[1];
			outputStreams[0] = outputStream;

			// Recurse over all children and create input streams array
			//
			int numInputs = rootLogicalNode.numInputs();

			Stream[] inputStreams = new Stream[numInputs];

			for (int child = 0; child < numInputs; ++child) {

				// Create a new input stream
				//
				inputStreams[child] = new Stream(streamCapacity);

				// Recurse on child
				//
				scheduleForExecution(rootLogicalNode.input(child),
									 inputStreams[child]);
			}

			// Instantiate operator with input and output streams.
			// The selected algorithm is instantiated
			//
			Class physicalOperatorClass = operator.getSelectedAlgo();

			// If there is no selected algo, error
			//
			if (physicalOperatorClass == null) {
				System.err.println("Error: No algorithm selected during execution scheduling");
				return;
			}

			System.out.println("Algorithm Name = " + physicalOperatorClass);

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

			// Put the new created physical operator in the operator queue
			//
			opQueue.putOperator(physicalOperator);
		}
    }


    /**
     * This function processes a DTD Scan Logical Operator by contacting the
     * data manager and scheduling it for execution
     *
     * @param dtdScanOperator The dtd scan operator that is to be scheduled
     *                        for execution
     * @param outputStream The stream to which the output of the scan operator
     *                     is to be fed
     */

    protected void processDTDScanOperator (dtdScanOp dtdScanOperator,
										   Stream outputStream) {

		// For now, we have the partial operator to add - THIS WILL GO
		//
		Stream[] outputStreams = new Stream[1];
		outputStreams[0] = outputStream;

		Stream[] inputStreams = new Stream[1];
		inputStreams[0] = new Stream(streamCapacity);

		PhysicalPartialOperator partialOp = 
			new PhysicalPartialOperator(null,
										inputStreams,
										outputStreams,
										responsiveness);

		opQueue.putOperator(partialOp);

		// Ask the data manager to start filling the output stream with
		// the parsed XML documents
		//
		try {
            System.err.println("Try to scan " +
							   dtdScanOperator.getDocs().elementAt(0));
            boolean scan = 
				dataManager.getDocuments(dtdScanOperator.getDocs(),
										 null,
										 new SourceStream(inputStreams[0]));
            if(!scan) System.err.println("dtdScan FAILURE! " +
										 dtdScanOperator.getDocs().elementAt(0));
		}
		catch (Exception e) {
            e.printStackTrace();
			System.err.println("Data Manager Already Closed!!!");
		}
    }

    /**
     * This function processes a Firehose Scan Logical Operator by creating
     * a firehose thread to fetch data from the firehose, parse the
     * documents that arrive, and put them in the appropriate stream
     *
     * @param firehoseScanOperator The firehose scan operator that is to be scheduled
     *                        for execution
     * @param outputStream The stream to which the output of the scan operator
     *                     is to be fed
     */

    protected void processFirehoseScanOperator (FirehoseScanOp fhScanOp,
						Stream outputStream) {
	// For now, we have the partial operator to add - THIS WILL GO
	//
	Stream[] outputStreams = new Stream[1];
	outputStreams[0] = outputStream;
	
	Stream[] inputStreams = new Stream[1];
	inputStreams[0] = new Stream(streamCapacity);
	
	PhysicalPartialOperator partialOp = 
	    new PhysicalPartialOperator(null,inputStreams, 
					outputStreams, responsiveness);
	
	opQueue.putOperator(partialOp);
	
	
	// Ask the data manager to start filling the output stream with
	// the parsed XML documents
	//
	System.err.println("Attempting to start firehose ");

	FirehoseThread firehose = new FirehoseThread(fhScanOp.getSpec(),
						     new SourceStream(inputStreams[0]));

	
	// start the thread
	Thread fhthread = new Thread(firehose);
	fhthread.start();
    }


}

