
/**********************************************************************
  $Id: QueryResult.java,v 1.2 2000/08/09 23:54:00 tufte Exp $


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

import com.ibm.xml.parser.TXDocument;
import com.ibm.xml.parser.TXElement;
import org.w3c.dom.Node;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;


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
		public TXDocument result;
    }


    /////////////////////////////////////////////////////////////
    // These are the private data members of the class         //
    /////////////////////////////////////////////////////////////

    // The query id assigned by the query engine
    //
    private int queryId;

    // The output stream to get results from
    //
    private Stream outputStream;

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

    public QueryResult (int queryId, Stream outputStream) {

		// Initialize the query id
		//
		this.queryId = queryId;

		// Initialize the output stream
		//
		this.outputStream = outputStream;

		// Initially, no end of stream
		//
		this.endOfStream = false;

		// Initially, no error in execution
		//
		this.errorInExecution = false;

		// Initially, not generating partial results
		//
		this.generatingPartialResult = false;
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
		ResultsAlreadyReturnedException {

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
		throws ResultsAlreadyReturnedException {

		// Call the internal getNext operator with the timeout
		//
		try {
			return internalGetNext(timeout);
		}
		catch (java.lang.InterruptedException e) {

			// This will never happen
			//
			return null;
		}
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
		AlreadyReturningPartialException {

		// If end of stream, throw exception
		//
		if (endOfStream) {

			throw new ResultsAlreadyReturnedException();
		}

		// If partial results are already invoked, then raise an exception
		//
		if (generatingPartialResult) {

			throw new AlreadyReturningPartialException();
		}

		// Send a request for a partial result
		//
		try {
		    System.out.println("QR putting partial request down stream");
			outputStream.putControlElementDownStream(
				new StreamControlElement(StreamControlElement.GetPartialResult));
		}
		catch (NullElementException e) {
		}
		catch (StreamPreviouslyClosedException e) {
		}

		// Set the status of generating partial results
		//
		generatingPartialResult = true;
    }


    /**
     * This function kills a query
     */

    public void kill() { 
	
		// If the query is still active then kill it
		//
		if (!errorInExecution && !endOfStream) {

			// Dont worry about error, just attempt to put control message
			//
			try {
				outputStream.putControlElementDownStream(
					new StreamControlElement(StreamControlElement.ShutDown));
			}
			catch (NullElementException e) {
			}
			catch (StreamPreviouslyClosedException e) {
			}

			// Note that the query is in error
			//
			errorInExecution = true;
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
		throws java.lang.InterruptedException,
		ResultsAlreadyReturnedException {

		// Create a new result object
		//
		ResultObject resultObject = new ResultObject();

		// Check to make sure that stream has not been previously closed
		//
		if (endOfStream) {
			throw new ResultsAlreadyReturnedException();
		}

		// If there has been an error in execution, notify
		//
		if (errorInExecution) {
			resultObject.status = QueryError;
			return resultObject;
		}

		// Get the next element from the output stream
		//
		StreamElement resultElement;

		if (timeout >= 0) {

			// There is a valid timeout - so use it
			//
			resultElement = outputStream.getUpStreamElement(timeout);
		}
		else {

			// Block till the next result is got
			//
			resultElement = outputStream.getUpStreamElement();
		}


		// Now handle the various types of results
		//
		if (resultElement == null) {

			// Nothing read - so had timed out
			//
			processNullElement(resultObject);
		}
		else if (resultElement instanceof StreamControlElement) {

			// Process a control element read
			//
			processControlElement(resultObject, 
								  (StreamControlElement) resultElement);

		}
		else if (resultElement instanceof StreamTupleElement) {

			// Process a stream tuple element read
			//
			processTupleElement(resultObject,
								(StreamTupleElement) resultElement);
		}
		else if (resultElement instanceof StreamEosElement) {

			// Process a end of stream element
			//
			processEosElement(resultObject,
					  (StreamEosElement) resultElement);
		}
		else {

			System.err.println("Unknown result element from stream");
			System.exit(-1);
		}

		// Return the resultObject
		//
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

    private void processControlElement (ResultObject resultObject,
										StreamControlElement controlElement) {

		// Act based on the type of the control element
		//
		if (controlElement.type() == StreamControlElement.ShutDown) {

			// There is an error in the query
			//
			resultObject.status = QueryError;

			// Note the presence of error
			//
			errorInExecution = true;
		}
		else if (controlElement.type() ==
				 StreamControlElement.EndPartialResult) {

			// This is the end of a partial result
			//
			if (!generatingPartialResult) {
				System.err.println("Error: Partial Result when not expecting");
				System.exit(-1);
			}
	    
			// No more generating partial result
			//
			generatingPartialResult = false;
	    
			// Inform client
			//
			resultObject.status = EndOfPartialResult;
		}
		else if (controlElement.type() ==
				 StreamControlElement.SynchronizePartialResult) {
	    
			// This is a non-blocking result
			//
			if (!generatingPartialResult) {
				System.err.println("Error: Partial Result when not expecting");
				System.exit(-1);
			}
	    
			// No more generating partial result
			//
			generatingPartialResult = false;
	    
			// Inform client
			//
			resultObject.status = NonBlockingResult;
		}
		else {
	    
			System.err.println("Unknown Control Element");
			System.exit(-1);
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
		}
		else {
			resultObject.status = FinalQueryResult;
		}

		// Extract XML result
		//
		resultObject.result = extractXMLDocument(tupleElement);
    }


    /**
     * This function processes a end of stream element
     *
     * @param resultObject The result to be returned to the client
     * @param tupleElement The end of stream element read from the output stream
     */

    private void processEosElement (ResultObject resultObject,
									StreamEosElement eosElement) {

		// This is the end
		//
		resultObject.status = EndOfResult;

		// Note the end of results
		//
		endOfStream = true;
    }


    /**
     * This function extracts an XML document result from a tuple element
     *
     * @param tupleElement The tuple element from which the XML document is
     *                     extracted
     *
     * @return The extracted XML document
     */

    private TXDocument extractXMLDocument (StreamTupleElement tupleElement) {

		// First get the last attribute of the tuple
		//
		Object lastAttribute = tupleElement.getAttribute(tupleElement.size() - 1);
	
		// Create a TXDocument and add add the result to the TXDocument
		//
		TXDocument resultDocument;

		resultDocument = new TXDocument();

		resultDocument.appendChild(((Node) lastAttribute).cloneNode(true));

		// Return the result document
		//
		return resultDocument;
    }


    public String toString() {
		return ("Query Result Object for Query "+queryId);
    }
}
