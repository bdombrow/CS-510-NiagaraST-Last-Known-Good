
/**********************************************************************
  $Id: QueryResult.java,v 1.11 2003/02/26 06:35:12 tufte Exp $


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

import niagara.utils.*;
import niagara.ndom.*;
import niagara.connection_server.NiagraServer;

/**
 * QueryResult is the class at the client that interacts with the
 * output stream of the query
 *
 * @version 1.0
 *
 */

public class QueryResult {

    /////////////////////////////////////////////////////////////
    // These are exceptions thrown by members of this class    //
    /////////////////////////////////////////////////////////////

    /**
     * This exception is thrown when an attempt is made to get a partial
     * result when a request is already pending
     */

    public class AlreadyReturningPartialException extends Exception {

    }


    /**
     * This exception is thrown when an attempt is made to read results
     * from a previously closed stream
     */

    public class ResultsAlreadyReturnedException extends Exception {

    }


    /////////////////////////////////////////////////////////////
    // These are public static variables of this class         //
    /////////////////////////////////////////////////////////////

    /**
     * This return code implies that all the results have been returned
     */
    public static final int EndOfResult = 0;

    /**
     * This return code implies that all partial result have been returned
     */
    public static final int EndOfPartialResult = 1;

    /**
     * This return code implies that there was an error in executing
     * the query
     */
    public static final int QueryError = 2;

    /**
     * This return code implies that the query is non-blocking
     */
    public static final int NonBlockingResult = 3;

    /**
     * This return code implies the presence of a partial result
     */
    public static final int PartialQueryResult = 4;

    /**
     * This return code implies the presence of a final result
     */
    public static final int FinalQueryResult = 5;

    /**
     * This return code implies that a call timed out
     */
    public static final int TimedOut = 6;


    ///////////////////////////////////////////////////////////////////
    // These are public nested classes                               //
    ///////////////////////////////////////////////////////////////////

    /**
     * This is a class for returning result to the user
     */

    public class ResultObject {

		// The status of the request
		//
		public int status;

		// The value read
		//
		public Document result;
    }


    /////////////////////////////////////////////////////////////
    // These are the private data members of the class         //
    /////////////////////////////////////////////////////////////

    // The query id assigned by the query engine
    //
    private int queryId;

    // The output stream to get results from
    //
    private SourceTupleStream outputStream;

    // Variable to store whether end of stream has been detected
    //
    private boolean endOfStream;

    // Variable to store whether error in execution has been detected
    //
    private boolean errorInExecution;

    // Variable to store whether the output stream is in the process
    // of generating possibly partial results
    //
    private boolean generatingPartialResult;

    // for use when not sending results back to client, KT
    // actually, it isn't good to create new result objects
    // for every tuple anyway, but that is the least of our 
    // problems!!!
    private ResultObject dummyResultObject;

    /////////////////////////////////////////////////////////////
    // These are the methods of the class                      //
    /////////////////////////////////////////////////////////////

    /**
     * This is the constructor that is initialized with information about
     * the query id and the output stream
     *
     * @param queryId The query id issued by the system
     * @param outputStream The output stream of the query
     */

    public QueryResult (int queryId, PageStream outputPageStream) {

		// Initialize the query id
		//
		this.queryId = queryId;

		// Initialize the output stream
		//
		this.outputStream = new SourceTupleStream(outputPageStream);

		// Initially, no end of stream
		//
		this.endOfStream = false;

		// Initially, no error in execution
		//
		this.errorInExecution = false;

		// Initially, not generating partial results
		//
		this.generatingPartialResult = false;

		// for QUIET
		dummyResultObject = new ResultObject();
		dummyResultObject.status = FinalQueryResult;
    }


    /**
     * This function returns the query id assigned by the query engine
     *
     * @return query id assigned by the query engine
     */

    public int getQueryId () {

		return queryId;
    }


    /**
     * This function blocks and waits for the next result from the output
     * stream
     *
     * @return (a) The nature of the result: It could be either
     *             <code>QueryError</code> If error in executing query
     *             <code>EndOfResult</code> If all results have been consumed
     *             <code>EndOfPartialResult</code> If all partial results
     *                   have been sent
     *             <code>NonBlockingResult</code> If query results are non-blocking
     *             <code>QueryResult</code> An actual query result was read
     *         (b) The actual query result
     *
     * @exception java.lang.InterruptedException If thread is interrupted during
     *            execution
     * @exception ResultsAlreadyReturnedException If stream has returned all
     *            results already
     */

    public ResultObject getNext ()
	throws java.lang.InterruptedException,
	       ResultsAlreadyReturnedException,
	       ShutdownException {
	
	// Call the internal getNext operator without a timeout
	//
	return internalGetNext(-1);
    }


    /**
     * This function waits for the next result from the output stream for
     * the specified timeout interval.
     *
     * @param timeout The specified timeout interval
     *
     * @return (a) The nature of the result: It could be either
     *             <code>QueryError</code> If error in executing query
     *             <code>EndOfResult</code> If all results have been consumed
     *             <code>EndOfPartialResult</code> If all partial results
     *                   have been sent
     *             <code>NonBlockingResult</code> If query results are non-blocking
     *             <code>QueryResult</code> An actual query result was read
     *             <code>TimedOut</code> If the function timed out
     *         (b) The actual query result
     *
     * @exception ResultsAlreadyReturnedException If stream has returned all
     *            results already
     */

    public ResultObject getNext (int timeout)
	throws ResultsAlreadyReturnedException,
	       ShutdownException, InterruptedException {
	
	// Call the internal getNext operator with the timeout
	return internalGetNext(timeout);
    }


    /**
     * This function request partial results to be sent
     *
     * @exception ResultsAlreadyReturnedException If stream has returned
     *            all results already
     * @exception AlreadyReturningPartialException If a partial result
     *            request is pending
     */

    public void returnPartialResults ()
		throws ResultsAlreadyReturnedException,
		AlreadyReturningPartialException,
		ShutdownException {

		// If end of stream, throw exception
		if (endOfStream) {
		    throw new ResultsAlreadyReturnedException();
		}
		
		// If partial results are already invoked, 
		// then raise an exception
		if (generatingPartialResult) {
			throw new AlreadyReturningPartialException();
		}

		// Send a request for a partial result		
		System.out.println("QR putting partial request down stream");
		outputStream.putCtrlMsg(CtrlFlags.GET_PARTIAL, null);

		// Set the status of generating partial results
		generatingPartialResult = true;
    }


    /**
     * This function kills a query
     */

    public void kill() { 
	try {
	    // If the query is still active then kill it
	    if (!errorInExecution && !endOfStream) {	    
		// Dont worry about error, just attempt to put control message
		outputStream.putCtrlMsg(CtrlFlags.SHUTDOWN, "execution error");	 
		// Note that the query is in error
		errorInExecution = true;
	    }
	} catch(ShutdownException e) {
	    // ignore since we are killing query...
	}
    }

     
    /**
     * This function blocks and waits for the next result from the output stream.
     *
     * @return (a) The nature of the result: It could be either
     *             <code>QueryError</code> If error in executing query
     *             <code>EndOfResult</code> If all results have been consumed
     *             <code>EndOfPartialResult</code> If all partial results
     *                   have been sent
     *             <code>NonBlockingResult</code> If query results are non-blocking
     *             <code>QueryResult</code> An actual query result was read
     *             <code>TimedOut</code> If the function timed out
     *         (b) The actual query result
     *
     * @exception java.lang.InterruptedException If the thread was interrupted
     *            during execution. This happens only without a timeout
     * @exception ResultsAlreadyReturnedException If stream has returned all
     *            results already
     */

    public ResultObject internalGetNext (int timeout) 
	throws InterruptedException,
	       ResultsAlreadyReturnedException,
	       ShutdownException {
	
	// Create a new result object
	ResultObject resultObject;
	if(!NiagraServer.QUIET) {
	    resultObject = new ResultObject();
	} else {
	    resultObject = dummyResultObject;
	}

	// Check to make sure that stream has not been previously closed
	if (endOfStream) {
	    throw new ResultsAlreadyReturnedException();
	}
	
	// If there has been an error in execution, notify
	if (errorInExecution) {
	    resultObject.status = QueryError;
	    return resultObject;
	}

	// Get the next element from the output stream
	//
	StreamTupleElement tuple;
	
	if(timeout < 0) {
	    // neg or 0 implies no timeout, getTuple will block
	    // until next result received
	    timeout = 0; 
	}
	tuple = outputStream.getTuple(timeout);	
	
	// Now handle the various types of results
	if (tuple ==  null) {
	    if(outputStream.timedOut()) {
		// timed out 
		processNullElement(resultObject);
	    } else {
		// get and process the control message
		int ctrlFlag = outputStream.getCtrlFlag();
		processCtrlMessage(resultObject, ctrlFlag);
	    }
	} else {
	    // got a tuple
	    if(!NiagraServer.QUIET) {
		// Process a stream tuple element read
		processTupleElement(resultObject,tuple);
	    }
	} 
	
	// Return the resultObject
	return resultObject;
    }


    /**
     * This function processes the case when no result element is read
     *
     * @param resultObject The result to be returned to the client
     */

    private void processNullElement (ResultObject resultObject) {

		// Nothing read - so had timed out
		//
		resultObject.status = TimedOut;
    }


    /**
     * This function processes a control element read
     *
     * @param resultObject The result to be returned to the client
     */

    private void processCtrlMessage (ResultObject resultObject, 
				     int ctrlFlag) {
	// Act based on the type of the control message
	if (ctrlFlag == CtrlFlags.SHUTDOWN) {
	    // There is an error in the query
	    resultObject.status = QueryError;
	    
	    // Note the presence of error
	    errorInExecution = true;
	} else if (ctrlFlag == CtrlFlags.END_PARTIAL) {
	    // This is the end of a partial result
	    if (!generatingPartialResult) {
		throw new PEException("Error: Partial Result when not expecting");
	    }
	    
	    // No more generating partial result
	    generatingPartialResult = false;
	    
	    // Inform client
	    resultObject.status = EndOfPartialResult;
	} else if (ctrlFlag == CtrlFlags.SYNCH_PARTIAL) {
			// This is a non-blocking result
	    if (!generatingPartialResult) {
		throw new PEException("Error: Partial Result when not expecting");
	    }
	    
	    // No more generating partial result
	    generatingPartialResult = false;
	    
	    // Inform client=
	    resultObject.status = NonBlockingResult;
	} else if (ctrlFlag == CtrlFlags.EOS) {
	    // This is the end
	    resultObject.status = EndOfResult;
	    endOfStream = true;
	} else {
	    throw new PEException("unexpected control element");
	}
    }


    /**
     * This function processes a result tuple element
     *
     * @param resultObject The result to be returned to the client
     * @param tupleElement The tuple element read from the output stream
     */

    private void processTupleElement (ResultObject resultObject,
				      StreamTupleElement tupleElement) {

	// This is a result element, check if partial or final
	//
	if (tupleElement.isPartial()) {
	    resultObject.status = PartialQueryResult;
	} else {
	    resultObject.status = FinalQueryResult;
	}
	
	// Extract XML result
	//
	resultObject.result = extractXMLDocument(tupleElement);
    }

    /**
     * This function extracts an XML document result from a tuple element
     *
     * @param tupleElement The tuple element from which the XML document is
     *                     extracted
     *
     * @return The extracted XML document
     */

    private Document extractXMLDocument (StreamTupleElement tupleElement) {
		// First get the last attribute of the tuple
		Node lastAttribute = tupleElement.getAttribute(tupleElement.size() - 1);
	
		if(lastAttribute instanceof Document) {
		    return (Document)lastAttribute;
		} else if (lastAttribute instanceof Element) {
		    // Create a Document and add add the result to the Doc
		    Document resultDocument = DOMFactory.newDocument();
		    Node n = DOMFactory.importNode(resultDocument, 
						   lastAttribute);
		    resultDocument.appendChild(n);
		    return resultDocument;
		} else if (lastAttribute instanceof Attr) {
		    Document resultDocument = DOMFactory.newDocument();
		    // create an element from the attribute
		    Element newElt = 
			resultDocument.createElement(((Attr)lastAttribute)
						     .getName());
		    Text txt =
			resultDocument.createTextNode(((Attr)lastAttribute)
						      .getValue());
		    newElt.appendChild(txt);
		    resultDocument.appendChild(newElt);
		    return resultDocument;
		} else {
		    throw new PEException("KT What did I get??");
		}
    }


    public String toString() {
		return ("Query Result Object for Query "+queryId);
    }
}
