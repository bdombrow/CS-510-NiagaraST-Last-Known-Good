/**********************************************************************
  $Id: ExecutionScheduler.java,v 1.14 2002/05/23 06:31:41 vpapad Exp $


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
					       QueryInfo queryInfo) 
	throws ShutdownException {
	// First create a Physical Head Operator to handle this query
	// in the system, only need to do this when top node
	// is SourceOp and can't function as head

	// where the top node of optimizedTree should put its output
	PageStream opTreeOutput;
	if(optimizedTree.getOperator().isSourceOp()) {
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
			     new Hashtable(), DOMFactory.newDocument(),
			     queryInfo);
    }

    public PageStream scheduleSubPlan(logNode rootNode) 
	throws ShutdownException {
        if (debug) {
            System.err.println("Scheduling: "); 
            rootNode.dump();
        }
        
        if (rootNode.isSchedulable()) {
            PageStream results = new PageStream("SubPlan");
            scheduleForExecution(rootNode, results, new Hashtable(), 
				 DOMFactory.newDocument(), null);
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
				       PageStream outputStream,
				       Hashtable nodesScheduled,
                                       Document doc, QueryInfo queryInfo) 
	throws ShutdownException {
	// Get the operator corresponding to the logical node
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
                in.close();
            
		// Replace the subplan with a ReceiveOp
		ReceiveOp recv = new ReceiveOp();
		recv.setReceive(location, query_id);
		logNode rn = new logNode(recv);
		scheduleForExecution(rn, outputStream, nodesScheduled, doc, null);
		return;
	    } catch(MalformedURLException mue) {
		System.err.println("Malformed URL " + mue.getMessage() + " url " + url_location);
		mue.printStackTrace();
	    } catch(IOException ioe) {
		System.err.println("Execution scheduler: io exception " + ioe.getMessage());
		ioe.printStackTrace();
	    }
	}

	// kt - think above is mutant stuff, this is the start of
	// normal processing here
	op operator = rootLogicalNode.getOperator();
	Object po = operator;		

	if (nodesScheduled.containsKey(rootLogicalNode)) {
	    // This operator was already scheduled
	    // Just add outputStream to its output streams
	    PhysicalOperator physOp = 
		(PhysicalOperator) nodesScheduled.get(rootLogicalNode);
	    int i;
	    // UUUUGLY - KT FIX
	    for (i = 0; i < operator.getNumberOfOutputStreams(); i++) {
		if (physOp.getSinkStream(i) == null)
		    break;
	    }
	    physOp.setSinkStream(i, new SinkTupleStream(outputStream));
	    if (i == operator.getNumberOfOutputStreams() -1)
		opQueue.putOperator(physOp);
	    return;
	}

	// Handle the scan operators differently - these are operators
	// that can only appear at the very bottom of a query try and
	// provide input for the query
	if(operator.isSourceOp()) {
	    // all source ops use SinkTupleStreams for output and these
	    // streams must reflect GET_PARTIALS 
	    SinkTupleStream sinkStream = 
		new SinkTupleStream(outputStream, true);
	    if (operator instanceof dtdScanOp) {
		processDTDScanOperator((dtdScanOp) operator, sinkStream);
	    } else if (operator instanceof FirehoseScanOp) {
		processFirehoseScanOperator((FirehoseScanOp) operator, 
					    sinkStream);
	    } else if (operator instanceof StreamScanOp) {
		processStreamScanOperator((StreamScanOp) operator, sinkStream);
	    } else if (operator instanceof ConstantOp) {
		processConstantOp((ConstantOp) operator, sinkStream);
	    } else if (operator instanceof ReceiveOp) {
		processReceiveOp((ReceiveOp) operator, sinkStream);
	    } else {
		throw new PEException("Unexpected Source Op");
	    }
	} else {
	    // This is a regular operator node ... Create the output streams
	    // array
	    SinkTupleStream[] outputStreams = 
		new SinkTupleStream[operator.getNumberOfOutputStreams()];
	    outputStreams[0] = new SinkTupleStream(outputStream);
	    
	    // Recurse over all children and create input streams array
	    int numInputs = rootLogicalNode.numInputs();
	    
	    SourceTupleStream[] inputStreams = 
		new SourceTupleStream[numInputs];
	    
	    for (int child = 0; child < numInputs; ++child) {
		// Create a new input stream
		PageStream inputPageStream = new PageStream("To: " +
						  rootLogicalNode.getName());
		inputStreams[child] = new SourceTupleStream(inputPageStream);
		
		// Recurse on child
		scheduleForExecution(rootLogicalNode.input(child),
				     inputPageStream,
				     nodesScheduled, doc, null);
	    }
	    
	    // Instantiate operator with input and output streams.
	    // The selected algorithm is instantiated
	    Class physicalOperatorClass = operator.getSelectedAlgo();
	    // If there is no selected algo, error
	    //
	    if (physicalOperatorClass == null) {
		throw new PEException("No algorithm selected during execution scheduling");
	    }
	    
	    // Create a new instance of the class with the logical operator,
	    // input, output streams and responsiveness. First get the
	    // constructors
	    Constructor[] constructors = physicalOperatorClass.getConstructors();
	    
	    // Now create an object array of the parameters for the
	    // constructor
	    Object[] parameters = new Object[] {operator, inputStreams,
						outputStreams, responsiveness};
	    
	    // Create a new physical operator object
	    PhysicalOperator physicalOperator;
	    
	    try {
		physicalOperator = 
		    (PhysicalOperator) constructors[0].newInstance(parameters);
	    }
	    catch (DOMException de) {
		System.err.println("DOMException "+"Error code is" + de.code);
		throw new PEException("Error in Instantiating Physical Operator" + de.getMessage());
	    } catch (InstantiationException e) {
		throw new PEException("Error in Instantiating Physical Operator" + e.getMessage());
	    } catch (IllegalAccessException e) {
		throw new PEException("Error in Instantiating Physical Operator" + e.getMessage());
	    } catch (java.lang.reflect.InvocationTargetException e) {
	    throw new PEException("Error in Instantiating Physical Operator" + e.getMessage());
	    }

	    if(rootLogicalNode.isHead()) {
		physicalOperator.setAsHead(queryInfo);
	    }
	    if (physicalOperator.isReady()) {
		// KT FIX - call this only if necessary
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
					   SinkTupleStream outputStream) 
    throws ShutdownException {
	// Ask the data manager to start filling the output stream with
	// the parsed XML documents
	//
	boolean scan = 
	    dataManager.getDocuments(dtdScanOperator.getDocs(), 
				     null, outputStream);
	if(!scan) 
	    System.err.println("dtdScan FAILURE! " 
			       + dtdScanOperator.getDocs().elementAt(0));
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
						SinkTupleStream outputStream) {
	/* Create a FirehoseThread which will connect to the appropriate
	 * firehose and start reading documents from that firehose and
	 * putting them into the output stream
	 */
	FirehoseThread firehose = new FirehoseThread(fhScanOp.getSpec(),
						     outputStream);
	
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
					      SinkTupleStream outputStream) {
	
	/* Create a StreamThread which will connect to the appropriate
	 * stream (file or socket) and start reading documents from that 
	 * stream and put them into the output stream
	 */
	StreamThread stream = new StreamThread(sScanOp.getSpec(), 
					       outputStream);
	
	// start the thread
	Thread sthread = new Thread(stream);
	sthread.start();
    }

    protected void processConstantOp (ConstantOp op,
				      SinkTupleStream outputStream) {
	/* Create a ConstantOpThread */
	ConstantOpThread cot = new ConstantOpThread(op.getContent(),
						    outputStream);
	
	// start the thread
	Thread t = new Thread(cot);
	t.start();
    }

    protected void processReceiveOp (ReceiveOp op,
                                     SinkTupleStream outputStream) {
	/* Create a ReceiveThread which will connect to the appropriate
	 * Niagara engine and start reading tuples and putting them 
         * into the output stream
	 */
	ReceiveThread rt = new ReceiveThread(op, outputStream);
	
	// start the thread
	Thread t = new Thread(rt);
	t.start();
    }

}

