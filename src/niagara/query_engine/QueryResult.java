package niagara.query_engine;

import niagara.connection_server.ResultTransmitter;
import niagara.ndom.DOMFactory;
import niagara.utils.BaseAttr;
import niagara.utils.ControlFlag;
import niagara.utils.PEException;
import niagara.utils.PageStream;
import niagara.utils.ShutdownException;
import niagara.utils.SourceTupleStream;
import niagara.utils.Tuple;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 * QueryResult is the class at the client that interacts with the output stream
 * of the query
 * 
 * @version 1.0
 * 
 */

public class QueryResult {

	/**
	 * This exception is thrown when an attempt is made to get a partial result
	 * when a request is already pending
	 */

	@SuppressWarnings("serial")
	public class AlreadyReturningPartialException extends Exception {

	}

	/**
	 * This is a class for returning result to the user
	 */

	public class ResultObject {
		public Document result; // the value read from stream
		public boolean isPartial; // is result partial or final
		public String stringResult;
		public boolean sendStr;
		public boolean isPunctuation;

		public ResultObject() {
			;
		}

		public ResultObject(boolean sendStr) {
			this.sendStr = sendStr;
		}

	}

	// ///////////////////////////////////////////////////////////
	// These are the private data members of the class //
	// ///////////////////////////////////////////////////////////

	// The query id assigned by the query engine
	private int queryId;

	// The output stream to get results from
	private SourceTupleStream outputStream;
	private PageStream outputPageStream;

	// Variable to store whether the output stream is in the process
	// of generating possibly partial results
	private boolean generatingPartialResult;

	/**
	 * This is the constructor that is initialized with information about the
	 * query id and the output stream
	 * 
	 * @param queryId
	 *            The query id issued by the system
	 * @param outputStream
	 *            The output stream of the query
	 */

	public QueryResult(int queryId, PageStream outputPageStream) {
		this.queryId = queryId;
		this.outputPageStream = outputPageStream;
		this.outputStream = new SourceTupleStream(outputPageStream);
		this.generatingPartialResult = false;
	}

	/**
	 * This function returns the query id assigned by the query engine
	 * 
	 * @return query id assigned by the query engine
	 */

	public int getQueryId() {
		return queryId;
	}

	public PageStream getOutputPageStream() {
		return outputPageStream;
	}

	// KT - hack so I don't have to put ResultObject in it's own class,
	// as it is I'm removing about 10,000 extra allocations, so I get
	// one teeny weeny hack
	public ResultObject getNewResultObject(boolean sendStr) {
		return new ResultObject(sendStr);
	}

	/**
	 * This function blocks and waits for the next result from the output stream
	 * 
	 * @exception java.lang.InterruptedException
	 *                If thread is interrupted during execution
	 * 
	 * @returns Returns the control flag received from stream
	 */
	public ControlFlag getNextResult(ResultObject resultObject)
			throws java.lang.InterruptedException, ShutdownException {
		// -1 indicates no timeout - infinite timeout
		return internalGetNext(-1, resultObject);
	}

	/**
	 * This function waits for the next result from the output stream for the
	 * specified timeout interval.
	 * 
	 * @param timeout
	 *            The specified timeout interval
	 * @param resultObject
	 *            An object to be filled in with the result
	 * 
	 */
	public ControlFlag getNextResult(int timeout, ResultObject resultObject)
			throws ShutdownException, InterruptedException {
		return internalGetNext(timeout, resultObject);
	}

	/**
	 * This function request partial results to be sent
	 * 
	 * @exception AlreadyReturningPartialException
	 *                If a partial result request is pending
	 */

	public void requestPartialResult() throws AlreadyReturningPartialException,
			ShutdownException {

		// If partial results are already invoked,
		// then raise an exception
		if (generatingPartialResult) {
			throw new AlreadyReturningPartialException();
		}

		// Send a request for a partial result
		System.out.println("QR putting partial request down stream");
		// May return EOS, throw ShutdonwnEx or return NULLFLAG
		// Think I can ignore EOS...famous last words
		ControlFlag ctrlFlag = outputStream.putCtrlMsg(ControlFlag.GET_PARTIAL,
				null, null);

		assert (ctrlFlag == ControlFlag.EOS || ctrlFlag == ControlFlag.NULLFLAG) : "Unexpected control flag";

		// Set the status of generating partial results
		generatingPartialResult = true;
	}

	/**
	 * This function kills a query
	 */

	public void kill() {
		try {
			// Attempt to kill the query - best effort - ignore errors

			// return from outputStream.putCtrlMsg could be:
			// NULLFLAG, SHUTDOWN, SYNCH_PARTIAL, END_PARTIAL, EOS
			// can ignore all of them
			outputStream.putCtrlMsg(ControlFlag.SHUTDOWN, "kill query", null);
		} catch (ShutdownException e) {
			// ignore since we are killing query...
		}
	}

	/**
	 * send a request for buffer flush down stream - we've been waiting too long
	 * for results
	 * 
	 * @returns a result status, similar to what is in resultObject.status
	 */
	public ControlFlag requestBufFlush() throws ShutdownException {
		return outputStream.putCtrlMsg(ControlFlag.REQUEST_BUF_FLUSH, null, null);
	}

	/**
	 * This function blocks and waits for the next result from the output
	 * stream.
	 * 
	 * @param timeout
	 *            Amount of time to sleep on query output stream
	 * @param resultObject
	 *            Object to be filled in with result
	 * 
	 * @return control flag from stream
	 * 
	 * @exception java.lang.InterruptedException
	 *                If the thread was interrupted during execution. This
	 *                happens only without a timeout
	 */

	private ControlFlag internalGetNext(int timeout, ResultObject resultObject)
			throws InterruptedException, ShutdownException {

		// Get the next element from the query output stream
		resultObject.result = null;

		if (timeout < 0) {
			// neg or 0 implies no timeout, we use
			// maxDelay, so we get tuples even from slow streams
			timeout = PageStream.MAX_DELAY;
		}

		Tuple tuple = outputStream.getTuple(timeout);
		ControlFlag ctrlFlag = outputStream.getCtrlFlag();

		// Now handle the various types of results
		if (tuple == null) {
			// process the control message
			if (ctrlFlag == ControlFlag.END_PARTIAL
					|| ctrlFlag == ControlFlag.SYNCH_PARTIAL) {
				assert generatingPartialResult : "Unexpected partial result";
				generatingPartialResult = false;
			}
		} else {
			assert ctrlFlag == ControlFlag.NULLFLAG : "Unexpected control flag "
					+ ctrlFlag.flagName();
			resultObject.isPunctuation = tuple.isPunctuation();
			resultObject.isPartial = tuple.isPartial();
			if (!resultObject.sendStr)
				resultObject.result = extractXMLDocument(tuple);
			else
				resultObject.stringResult = extractResult(tuple);
		}
		return ctrlFlag;
	}

	private String extractResult(Tuple tupleElement) {
		if (ResultTransmitter.OUTPUT_FULL_TUPLE) {

			int tupleSize = tupleElement.size();

			if (tupleSize == 1) {
				if (tupleElement.getAttribute(0) instanceof Node) {
					if (((Node) tupleElement.getAttribute(0)).getNodeType() == Node.DOCUMENT_NODE)
						return null; // (Document)tupleElement.getAttribute(0);
				} else if (tupleElement.getAttribute(0) instanceof BaseAttr) {
					return ((BaseAttr) tupleElement.getAttribute(0)).toASCII(); // tupleAttrToDoc((BaseAttr)tupleElement.getAttribute(0));
				} else
					throw new PEException(
							"JL: Unsupported tuple attribute type");

			} else {
				int startIdx = 0;
				if (tupleElement.getAttribute(0) instanceof Node)
					if (((Node) tupleElement.getAttribute(0)).getNodeType() == Node.DOCUMENT_NODE)
						startIdx = 1;

				// Document resultDoc = DOMFactory.newDocument();
				// Element root = resultDoc.createElement("niagara:tuple");
				// resultDoc.appendChild(root);
				StringBuffer resultTuple = new StringBuffer();
				for (int i = startIdx; i < tupleSize; i++) {
					Object tupAttr = tupleElement.getAttribute(i);
					if (tupAttr instanceof Node) {
						if (((Node) tupleElement.getAttribute(i)).getNodeType() == Node.DOCUMENT_NODE)
							continue;
						resultTuple.append(tupleAttrToStr((Node) tupAttr)
								+ "\t");

					} else if (tupAttr instanceof BaseAttr) {
						resultTuple.append(((BaseAttr) tupAttr).toASCII()
								+ "\t");
					} else if (tupAttr == null) {
						resultTuple.append("null");
					} else
						throw new PEException(
								"JL: Unsupported tuple attribute type");
				}
				return resultTuple.append("\n").toString();
			}
		}

		Object lastAttribute = tupleElement
				.getAttribute(tupleElement.size() - 1);

		if (lastAttribute instanceof Node)
			return tupleAttrToStr(((Node) lastAttribute)) + "\n";
		else
			return ((BaseAttr) lastAttribute).toASCII() + "\n";

	}

	private Document extractXMLDocument(Tuple tupleElement) {
		// First get the last attribute of the tuple

		if (ResultTransmitter.OUTPUT_FULL_TUPLE) {

			int tupleSize = tupleElement.size();

			if (tupleSize == 1) {
				if (tupleElement.getAttribute(0) instanceof Node) {
					if (((Node) tupleElement.getAttribute(0)).getNodeType() == Node.DOCUMENT_NODE)
						return (Document) tupleElement.getAttribute(0);
				} else if (tupleElement.getAttribute(0) instanceof BaseAttr) {
					return tupleAttrToDoc((BaseAttr) tupleElement
							.getAttribute(0));
				} else
					throw new PEException(
							"JL: Unsupported tuple attribute type");

			} else {
				int startIdx = 0;
				if (tupleElement.getAttribute(0) instanceof Node)
					if (((Node) tupleElement.getAttribute(0)).getNodeType() == Node.DOCUMENT_NODE)
						startIdx = 1;

				Document resultDoc = DOMFactory.newDocument();
				Element root = resultDoc.createElement("niagara:tuple");
				resultDoc.appendChild(root);
				for (int i = startIdx; i < tupleSize; i++) {
					Object tupAttr = tupleElement.getAttribute(i);
					if (tupAttr instanceof Node) {
						if (((Node) tupleElement.getAttribute(i)).getNodeType() == Node.DOCUMENT_NODE)
							continue;
						Element elt = tupleAttrToElt((Node) tupAttr, resultDoc);
						root.appendChild(elt);
					} else if (tupAttr instanceof BaseAttr) {
						Element elt = tupleAttrToElt((BaseAttr) tupAttr,
								resultDoc);
						root.appendChild(elt);
					} else if (tupAttr == null) {
						Element elt = resultDoc.createElement("null");
						root.appendChild(elt);
					} else
						throw new PEException(
								"JL: Unsupported tuple attribute type");
				}
				return resultDoc;
			}
		}

		Object lastAttribute = tupleElement
				.getAttribute(tupleElement.size() - 1);
		if (lastAttribute instanceof Node)
			return tupleAttrToDoc((Node) lastAttribute);
		else
			return tupleAttrToDoc((BaseAttr) lastAttribute);
	}

	/*
	 * private Document extractXMLDocument(Tuple tupleElement) { // First get
	 * the last attribute of the tuple
	 * 
	 * if (ResultTransmitter.OUTPUT_FULL_TUPLE) {
	 * 
	 * int tupleSize = tupleElement.size();
	 * 
	 * if(tupleSize == 1 && tupleElement.getAttribute(0).getNodeType() ==
	 * Node.DOCUMENT_NODE) { return (Document)tupleElement.getAttribute(0); }
	 * else { int startIdx = 0; if(tupleElement.getAttribute(0).getNodeType() ==
	 * Node.DOCUMENT_NODE) { startIdx = 1; } Document resultDoc =
	 * DOMFactory.newDocument(); Element root =
	 * resultDoc.createElement("niagara:tuple"); resultDoc.appendChild(root);
	 * for (int i = startIdx; i < tupleSize; i++) { Node tupAttr =
	 * tupleElement.getAttribute(i); Element elt = tupleAttrToElt(tupAttr,
	 * resultDoc); root.appendChild(elt); } return resultDoc; } }
	 * 
	 * Node lastAttribute = tupleElement.getAttribute(tupleElement.size() - 1);
	 * return tupleAttrToDoc(lastAttribute); }
	 */

	private Document tupleAttrToDoc(Node tupAttr) {

		if (tupAttr instanceof Document) {
			return (Document) tupAttr;
		} else {
			Document resultDoc = DOMFactory.newDocument();
			Element elt = tupleAttrToElt(tupAttr, resultDoc);
			resultDoc.appendChild(elt);
			return resultDoc;
		}
	}

	private Document tupleAttrToDoc(BaseAttr tupAttr) {
		Document resultDoc = DOMFactory.newDocument();
		Element elt = tupleAttrToElt(tupAttr, resultDoc);
		resultDoc.appendChild(elt);
		return resultDoc;
	}

	private Element tupleAttrToElt(Node tupAttr, Document resultDoc) {

		if (tupAttr instanceof Element) {
			return (Element) DOMFactory.importNode(resultDoc, tupAttr);
		} else if (tupAttr instanceof Attr) {
			Element newElt = resultDoc
					.createElement(((Attr) tupAttr).getName());
			Text txt = resultDoc.createTextNode(((Attr) tupAttr).getValue());
			newElt.appendChild(txt);
			return newElt;
		} else if (tupAttr instanceof Text) {
			Element newElt = resultDoc.createElement("Text");
			Text txt = resultDoc.createTextNode(tupAttr.getNodeValue());
			newElt.appendChild(txt);
			return newElt;
		} else if (tupAttr == null) {
			return resultDoc.createElement("niagara:null");
		} else {
			assert false : "KT What did I get?? "
					+ tupAttr.getClass().getName();
			return null;
		}
	}

	private String tupleAttrToStr(Node tupAttr) {
		if (tupAttr instanceof Element) {
			StringBuffer result = new StringBuffer();
			Element elt = (Element) tupAttr;
			Node item;
			int size = elt.getChildNodes().getLength();
			for (int i = 0; i < size; i++) {
				item = elt.getChildNodes().item(i);
				if (item.getNodeType() == Node.ELEMENT_NODE)
					result.append(item.getFirstChild().getNodeValue() + "\t");
				else
					result.append(item.getNodeValue() + "\t");
			}
			return result.toString();
			// return ((Element)tupAttr).getFirstChild().getNodeValue();
		} else if (tupAttr instanceof Attr) {
			return ((Attr) tupAttr).getValue();
		} else if (tupAttr instanceof Text) {
			return tupAttr.getNodeValue();
		} else if (tupAttr == null) {
			return null;
		} else {
			assert false : "KT What did I get?? "
					+ tupAttr.getClass().getName();
			return null;
		}
	}

	private Element tupleAttrToElt(BaseAttr tupAttr, Document resultDoc) {
		Element elt = resultDoc.createElement("BaseAttr");

		Text txt = resultDoc.createTextNode(tupAttr.toASCII());
		elt.appendChild(txt);
		return elt;
	}

	public String toString() {
		return ("Query Result Object for Query " + queryId);
	}
}
