 
/**********************************************************************
  $Id: ExecutionScheduler.java,v 1.10 2002/03/26 23:52:31 tufte Exp $


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

import org.w3c.dom.*;

import java.lang.reflect.Constructor;
import java.util.Vector;
import java.util.Hashtable;
import niagara.data_manager.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.ndom.*;

import niagara.connection_server.NiagraServer;
import niagara.connection_server.MQPHandler;

import java.io.*;
import java.net.*;

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

    // The server we're running in
    private NiagraServer server;

    // This is the data manager that the physical operators interact with
    //
    private DataManager dataManager;

    // This is the operator queue in which all operators scheduled for
    // execution are to be put
    //
    private PhysicalOperatorQueue opQueue;

    // This specifies the responsiveness of operators
    //
    private static Integer responsiveness = new Integer(100);

    private boolean debug = false;

    //////////////////////////////////////////////////////////////////
    // These are the methods of the class                           //
    //////////////////////////////////////////////////////////////////

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
		inputStreams[0] = new Stream();

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
		//System.err.println("ES:: optimizedTree is: ");
		//optimizedTree.dump();

		scheduleForExecution(optimizedTree, inputStreams[0], 
				     new Hashtable(), DOMFactory.newDocument());
    }

    public Stream scheduleSubPlan(logNode rootNode) {
        if (debug) {
            System.err.println("Scheduling: "); 
            rootNode.dump();
        }
        
        if (rootNode.isSchedulable()) {
            Stream results = new Stream();
            scheduleForExecution(rootNode, results, new Hashtable(), DOMFactory.newDocument());
            return results;
        }
        else { // A mutant query plan
            MQPHandler handler = new MQPHandler(this, rootNode);
            handler.start();
            return null;
        }
    }

    /**
     * This function schedules the DAG rooted at "rootLogicalNode" for
     * execution
     *
     * @param rootLogicalNode The root of the the logical tree to be
     *                        scheduled for execution
     * @param outputStream    The stream to which the output is to be sent
     * @param nodesScheduled  A hashtable containing all the logical plan nodes
     *                        that are already scheduled, since 
     *                        the plan is not necessarily a tree.
     * @param doc             The DOM document that will own any newly created XML nodes
     */

    private void scheduleForExecution (logNode rootLogicalNode,
				       Stream outputStream,
				       Hashtable nodesScheduled,
                                       Document doc) {
	
	// Get the operator corresponding to the logical node
	//

        // Check if this is the root of a subplan meant to run
        // on a different engine
        //System.out.println("XXX Scheduling node: " + rootLogicalNode);
        
        String location = rootLogicalNode.getLocation();

        if (location != null && !location.equals(server.getLocation())) {

            String url_location = "http://" + location + "/servlet/communication";
            rootLogicalNode.setLocation(null);
            
            String query_id = "";

            try {
                String encodedQuery = URLEncoder.encode(
                    rootLogicalNode.subplanToXML());

                //System.err.println("ES: Getting subplan id...");
                URL url = new URL(url_location);
                URLConnection connection = url.openConnection();
                connection.setDoOutput(true);
                PrintWriter out = new PrintWriter(connection.getOutputStream());
                out.println("type=submit_subplan&query=" + encodedQuery);
                out.flush();
                out.close();

                BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                        connection.getInputStream()));
                query_id = in.readLine();

                //System.err.println("ES: Id of remote plan is: " + query_id);

                in.close();
            }
            catch (Exception e) {
                System.out.println("ES: Exception while requesting id from remote engine:");
                e.printStackTrace();
                System.exit(-1);
            }
            
            // Replace the subplan with a ReceiveOp
            ReceiveOp recv = null;
            try {
                recv = (ReceiveOp) operators.receive.clone();
            }
            catch (Exception e) {
                System.err.println("Could not clone receive op!");
            }
            recv.setReceive(location, query_id);
            logNode rn = new logNode(recv);
            scheduleForExecution(rn, outputStream, nodesScheduled, doc);
            return;
        }

	op operator = rootLogicalNode.getOperator();
	Object po = operator;		

	if (nodesScheduled.containsKey(rootLogicalNode)) {
	    // This operator was already scheduled
	    // Just add outputStream to its output streams
	    PhysicalOperator physOp = (PhysicalOperator) nodesScheduled.get(rootLogicalNode);
	    Stream s = null;
	    int i;
	    for (i = 0; i < operator.getNumberOfOutputStreams(); i++) {
		if (physOp.getDestinationStream(i) == null)
		    break;
	    }
	    physOp.setDestinationStream(i, outputStream);
	    if (i == operator.getNumberOfOutputStreams() -1)
		opQueue.putOperator(physOp);
	    return;
	}

		// If this is a DTD Scan operator, then process accordingly
		//
		if (operator instanceof dtdScanOp) {
		    processDTDScanOperator((dtdScanOp) operator, outputStream);
		} else if (operator instanceof FirehoseScanOp) {
		    processFirehoseScanOperator((FirehoseScanOp) operator, 
						outputStream);
		} else if (operator instanceof StreamScanOp) {
		    processStreamScanOperator((StreamScanOp) operator, 
					      outputStream);
		} else if (operator instanceof ConstantOp) {
                    processConstantOp((ConstantOp) operator, outputStream);
		} else if (operator instanceof ReceiveOp) {
                    processReceiveOp((ReceiveOp) operator, outputStream);
                }

                else {
			// This is a regular operator node ... Create the output streams
			// array
			//
			Stream[] outputStreams = new Stream[operator.getNumberOfOutputStreams()];
			outputStreams[0] = outputStream;

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
				scheduleForExecution(rootLogicalNode.input(child),
						     inputStreams[child],
						     nodesScheduled, doc);
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

			//System.out.println("Algorithm Name = " + physicalOperatorClass);

			// Create a new instance of the class with the logical operator,
			// input, output streams and responsiveness. First get the
			// constructors
			//
			Constructor[] constructors = physicalOperatorClass.getConstructors();

			// Now create an object array of the parameters for the
			// constructor
			//
			Object[] parameters = new Object[] {operator, inputStreams,
                                                            outputStreams, responsiveness};

			// Create a new physical operator object
			//
			PhysicalOperator physicalOperator;

			try {
				physicalOperator = 
					(PhysicalOperator) constructors[0].newInstance(parameters);
			}
			catch (Exception e) {
				System.err.println("Error in Instantiating Physical Operator");
			    if(e instanceof DOMException) {
				System.err.println("e is a DOMException");
				System.err.println("Error code is" + ((DOMException)e).code);
			    }

				e.printStackTrace();
				return;
			}

			if (physicalOperator.isReady()) {
                            physicalOperator.setResultDocument(doc);
			    // Put the new created physical operator in the operator queue
			    opQueue.putOperator(physicalOperator);
			}
			nodesScheduled.put(rootLogicalNode, physicalOperator);
			return;
		}
		nodesScheduled.put(rootLogicalNode, po);
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
		inputStreams[0] = new Stream();

		PhysicalPartialOperator partialOp = 
		    new PhysicalPartialOperator(null, inputStreams,
						outputStreams, 
						responsiveness);

		opQueue.putOperator(partialOp);

		// Ask the data manager to start filling the output stream with
		// the parsed XML documents
		//
		try {
		    //System.err.println("XXX Try to scan " +
                    //	       dtdScanOperator.getDocs().elementAt(0));
		    boolean scan = 
			dataManager.getDocuments(dtdScanOperator.getDocs(), null,
						 new SourceStream(inputStreams[0]));
		    if(!scan) 
			System.err.println("dtdScan FAILURE! " 
					   + dtdScanOperator.getDocs().elementAt(0));
		}
		catch (Exception e) {
		    e.printStackTrace();
		    System.err.println("Data Manager Already Closed!!!");
		}
                //System.out.println("XXX returning from handleDtdScan");
    }

    /**
     * This function processes a Firehose Scan Logical Operator by creating
     * a firehose thread to fetch data from the firehose, parse the
     * documents that arrive, and put them in the appropriate stream
     *
     * @param firehoseScanOp The firehose scan operator to be scheduled
     *                        for execution
     * @param outputStream The stream to which the output of the scan operator
     *                     is to be fed
     */

    protected void processFirehoseScanOperator (FirehoseScanOp fhScanOp,
						Stream outputStream) {
	Stream[] outputStreams = new Stream[1];
	outputStreams[0] = outputStream;
	
	Stream[] inputStreams = new Stream[1];
	inputStreams[0] = new Stream();
	
	PhysicalPartialOperator partialOp = 
	    new PhysicalPartialOperator(null,inputStreams, 
					outputStreams, responsiveness);
	
	opQueue.putOperator(partialOp);
	
	
	/* Create a FirehoseThread which will connect to the appropriate
	 * firehose and start reading documents from that firehose and
	 * putting them into the output stream
	 */
	//System.err.println("Attempting to start firehose ");

	FirehoseThread firehose = new FirehoseThread(fhScanOp.getSpec(),
				      new SourceStream(inputStreams[0]));
	
	// start the thread
	Thread fhthread = new Thread(firehose);
	fhthread.start();
	return;
    }

    /**
     * This function processes a Stream Scan Logical Operator by creating
     * a stream thread to fetch data from the stream, parse the
     * documents that arrive, and put them in the appropriate stream
     *
     * @param streamScanOp The stream scan operator to be scheduled
     *                        for execution
     * @param outputStream The stream to which the output of the scan operator
     *                     is to be fed
     */

    protected void processStreamScanOperator (StreamScanOp sScanOp,
					      Stream outputStream) {
	Stream[] outputStreams = new Stream[1];
	outputStreams[0] = outputStream;
	
	Stream[] inputStreams = new Stream[1];
	inputStreams[0] = new Stream();
	
	PhysicalPartialOperator partialOp = 
	    new PhysicalPartialOperator(null,inputStreams, 
					outputStreams, responsiveness);
	
	opQueue.putOperator(partialOp);
	
	
	/* Create a StreamThread which will connect to the appropriate
	 * stream (file or socket) and start reading documents from that 
	 * stream and put them into the output stream
	 */

	StreamThread stream = new StreamThread(sScanOp.getSpec(),
				     new SourceStream(inputStreams[0]));
	
	// start the thread
	Thread sthread = new Thread(stream);
	sthread.start();
	return;
    }

    protected void processConstantOp (ConstantOp op,
						Stream outputStream) {
	Stream[] outputStreams = new Stream[1];
	outputStreams[0] = outputStream;
	
	Stream[] inputStreams = new Stream[1];
	inputStreams[0] = new Stream();
	
	PhysicalPartialOperator partialOp = 
	    new PhysicalPartialOperator(null,inputStreams, 
					outputStreams, responsiveness);
	
	opQueue.putOperator(partialOp);
	
	
	/* Create a ConstantOpThread */

	ConstantOpThread cot = new ConstantOpThread(op.getContent(),
				      new SourceStream(inputStreams[0]));
	
	// start the thread
	Thread t = new Thread(cot);
	t.start();
    }

    protected void processReceiveOp (ReceiveOp op,
                                     Stream outputStream) {
	Stream[] outputStreams = new Stream[1];
	outputStreams[0] = outputStream;
	
	Stream[] inputStreams = new Stream[1];
	inputStreams[0] = new Stream();
	
	PhysicalPartialOperator partialOp = 
	    new PhysicalPartialOperator(null,inputStreams, 
					outputStreams, responsiveness);
	
	opQueue.putOperator(partialOp);
	
	
	/* Create a ReceiveThread which will connect to the appropriate
	 * Niagara engine and start reading tuples and putting them 
         * into the output stream
	 */

	ReceiveThread rt = new ReceiveThread(op,
                                             new SourceStream(inputStreams[0]));
	
	// start the thread
	Thread t = new Thread(rt);
	t.start();
    }

}

