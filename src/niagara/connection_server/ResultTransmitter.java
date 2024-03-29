package niagara.connection_server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import niagara.query_engine.QueryResult;
import niagara.query_engine.Schedulable;
import niagara.utils.CPUTimer;
import niagara.utils.ControlFlag;
import niagara.utils.JProf;
import niagara.utils.PEException;
import niagara.utils.PageStream;
import niagara.utils.ShutdownException;

/**
 * This is the thread for transmitting out the results of a query to the client
 * Every query that gets submitted to the server has a corresponding
 * resultTranmitting thread
 */
@SuppressWarnings("unchecked")
public class ResultTransmitter implements Schedulable {
	// The queryInfo of the query to which this transmitter belongs
	private ServerQueryInfo queryInfo;
	private RequestMessage request;

	// this will be used to access the output stream to send out results
	private RequestHandler handler;

	// The results that are yet to be sent
	private int totalResults;

	// for suspending and resuming the thread
	// private boolean suspended = false;

	// The response message in construction
	private ResponseMessage response;

	// The number of results that have been collected so far in 'response'
	private int resultsSoFar = 0;

	// The size (in bytes) of a batch in a batched query
	private static final int BATCHSIZE = 1024;

	private final boolean sendImmediate;
	private final boolean intermittent;

	private final Object intermittentSynch = new Object();

	// Tags for element and attribute list
	// private static final String ELEMENT = "<!ELEMENT";
	// private static final String ATTLIST = "<!ATTLIST";

	// by default, print nicely, exception is when results
	// are coming directly from a generator in which case
	// prettyprint is handled by generator
	private boolean prettyprint = true;
	private boolean timedOut = false; // true if last get timed out
	private boolean killThread = false;
	private CPUTimer cpuTimer;

	public static boolean QUIET = false;
	public static boolean OUTPUT_FULL_TUPLE = false;
	public static boolean BUF_FLUSH = true;

	private Object queryDone = new Object();

	private ArrayList epochQueue;

	/**
	 * Constructor
	 * 
	 * @param handler
	 *            The request handler that created this transmitter
	 * @param queryInfo
	 *            The info about the query to be executed
	 * @param request
	 *            The request that contained the query
	 */
	public ResultTransmitter(RequestHandler handler, ServerQueryInfo queryInfo,
			RequestMessage request) {
		this.handler = handler;
		this.queryInfo = queryInfo;
		this.request = request;
		totalResults = 0;
		this.sendImmediate = request.isSendImmediate();
		this.intermittent = request.isIntermittent();
		epochQueue = new ArrayList();
	}

	public void setPrettyprint(boolean prettyprint) {
		this.prettyprint = prettyprint;
	}

	public void run() {
		try {
			if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
				if (cpuTimer == null)
					cpuTimer = new CPUTimer();
				cpuTimer.start();
			}

			if (queryInfo.isQEQuery()) {
				handleQEQuery();
			} else if (queryInfo.isAccumFileQuery()) {
				handleQEQuery();
				// handleAccumQuery();
			} else {
				assert false : "Invalid query type";
				return;
			}

			if (niagara.connection_server.NiagraServer.TIME_OPERATORS) {
				cpuTimer.stop();
				cpuTimer.print("ResultTransmitter ");
			}

			if (NiagraServer.RUNNING_NIPROF) {
				System.out.println("KT requesting data dump");
				System.gc();
				JProf.requestDataDump();
			}

		} catch (ShutdownException se) {
			// send error message to client...
			response = new ResponseMessage(request,
					ResponseMessage.EXECUTION_ERROR);
			response.setData("Message: " + se.getMessage());
			/*
			 * try { handler.sendResponse(response); } catch (IOException ioe) {
			 * // nothing to be done, we did our best System.err.println(
			 * "KT unable to send error message to client: " + se.getMessage());
			 * }
			 */

		} catch (InterruptedException ie) {
			// ditto...
		} catch (IOException ioe) {
			// probably means there was a problem sending message
			// to client
			System.err
					.println("KT ResultTransmitter - IO exception - likely problem sending message to client ["
							+ ioe.toString() + "]");
			queryInfo.getQueryResult().kill();
		}
		synchronized (queryDone) {
			queryDone.notifyAll();
		}
	}

	public Object getQueryDone() {
		return queryDone;
	}

	// for use by RequestHandler when client requests that this
	// query be killed
	public void destroy() {
		killThread = true;
	}

	// private void handleDTDQuery() throws IOException {
	// ResponseMessage response = new ResponseMessage(request,
	// ResponseMessage.DTD);
	// try {
	// URL url = new URL(request.requestData);
	// BufferedReader rd = new BufferedReader(new InputStreamReader(url
	// .openStream()));
	// response.setData("<![CDATA[");
	// response.appendData(assembleDTD(rd) + "]]>");
	// } catch (MalformedURLException e1) {
	// response = new ResponseMessage(request, ResponseMessage.ERROR);
	// response.setData("Bad Url for DTD");
	// } catch (IOException e2) {
	// response = new ResponseMessage(request, ResponseMessage.ERROR);
	// response.setData("Could Not Fetch the DTD");
	// }
	// // handler.sendResponse(response);
	// }

	/**
	 * helper to handle DTD Query
	 */
	// private String assembleDTD(BufferedReader r) throws IOException {
	// StringBuffer res = new StringBuffer();
	//
	// while (r.ready()) {
	// String line = r.readLine();
	//
	// if (line.indexOf(ELEMENT) != -1 || line.indexOf(ATTLIST) != -1
	// || line.indexOf("<!ENTITY") != -1) {
	// // add the line to the dtd
	// res.append(line);
	// // if the end > is not on this line
	// // continue to add lines until you find the >
	// if (line.indexOf(">") == -1) {
	// while (r.ready()) {
	// line = r.readLine();
	// res.append(line);
	// if (line.indexOf(">") != -1) {
	// break;
	// }
	// }
	// }
	// res.append("\n");
	// }
	//
	// }
	// return res.toString();
	// }

	/**
	 * Handles QE Query.
	 */
	private void handleQEQuery() throws ShutdownException,
			InterruptedException, IOException {
		QueryResult queryResult = queryInfo.getQueryResult();

		// XXX vpapad: Taking this out of the loop
		response = new ResponseMessage(request, ResponseMessage.QUERY_RESULT);

		// do the allocation here instead of inside getNext, so
		// we only allocate one resultObject
		QueryResult.ResultObject resultObject = queryResult
				.getNewResultObject(request.asii);

		while (true) {
			if (killThread) {
				return;
			}

			// get the next result (KT: gets one result)
			// KT HERE IS WHERE SERVER RESULTS ARE PRODUCED
			ControlFlag ctrlFlag = queryResult.getNextResult(
					PageStream.MAX_DELAY, resultObject);
			// REFACTOR:
			if (ctrlFlag != ControlFlag.NULLFLAG) {
				assert resultObject.result == null : "KT didn't think this should happen";
				synchronized (intermittentSynch) {
					processCtrlMsg(ctrlFlag);
				}
			} else {
				assert resultObject.result != null : "KT HELP!!!";

				// It is a query result, so save it
				// add the result to responseData
				totalResults--;
				resultsSoFar++;
				timedOut = false;

				if (!QUIET) {
					response.appendResultData(resultObject, prettyprint);
					if (intermittent) {
						if (resultObject.isPunctuation) {
							appendToEpochQ(new ResponseMessage(response));
							response.clearData();

							/*
							 * synchronized (intermittentSynch) { if (handler ==
							 * null) { //should toss data here, when there is a
							 * punctuation continue; } else { sendResults();
							 * closeEpoch(); } }
							 */
							/*
							 * synchronized (intermittentSynch) { while (handler
							 * == null) { intermittentSynch.wait(); } }
							 */
						}
					} else if (sendImmediate || response.dataSize() > BATCHSIZE) {
						sendResults();
					}
				}
			}
		}
	}

	/**
	 * Function to "handle" an Accumulate Query. This function is run in this
	 * class's thread and basically sits on top of a Accumulate Query. The
	 * function of this function/thread is to pull results from the accumulate
	 * operator and update the Accumulate File Directory.
	 */
	// private void handleAccumQuery() throws ShutdownException,
	// InterruptedException, IOException {
	//
	// assert false : "KT: should not be using this function for now";
	//
	// QueryResult queryResult = queryInfo.getQueryResult();
	// response = new ResponseMessage(request, ResponseMessage.QUERY_RESULT);
	//
	// QueryResult.ResultObject resultObject = queryResult
	// .getNewResultObject(request.asii);
	//
	// while (true) {
	// if (killThread) {
	// return;
	// }
	//
	// /*
	// * I removed all the suspension stuff and made it so that AccumFile
	// * queries can't be suspended because I don't fully understand
	// * suspension and because it doesn't seem necessary for AccumFile
	// * stuff
	// */
	//
	// /* get the result and update the accum file dir */
	// /* OK, now wait for the result to come popping up */
	// ControlFlag ctrlFlag = queryResult.getNextResult(
	// PageStream.MAX_DELAY, resultObject);
	// timedOut = false;
	//
	// if (ctrlFlag != ControlFlag.NULLFLAG) {
	// processCtrlMsg(ctrlFlag);
	// } else {
	// if (resultObject.isPartial) {
	// /*
	// * In this case, resultObject.result is a Document
	// * AccumFileDir stores standard DOM Docs, since that is what
	// * the system uses now
	// */
	// DataManager.AccumFileDir.put(queryInfo.getAccumFileName(),
	// resultObject.result);
	// } else {
	// // final result
	// DataManager.AccumFileDir.put(queryInfo.getAccumFileName(),
	// resultObject.result);
	//
	// /* send final results to client */
	//
	// if (!QUIET) {
	// response.appendData(getResultData(resultObject,
	// prettyprint));
	// sendResults();
	// }
	// }
	// }
	// }
	// }

	/**
	 * process a control message received from the stream possibilities for
	 * ctrlFlag are SYNCH_PARTIAL END_PARTIAL, EOS, TIMED_OUT
	 */
	private void processCtrlMsg(ControlFlag ctrlFlag)
			throws java.io.IOException, ShutdownException, InterruptedException {
		switch (ctrlFlag) {
		case EOS:

			if (!QUIET) {
				sendResults();
			}

			sendEndResult();

			// everything done! kill the query
			handler.killQuery(request.serverID);
			return;

		case TIMED_OUT:
			// if no more new results have come in a while
			// send the results and request more..
			if (!QUIET && !intermittent && timedOut
					&& !queryInfo.isAccumFileQuery()) {
				if (response.dataSize() != 0)
					// RJFM
					// System.out.println("Timed out twice & have results - sending them");
					// If we timed out two times in a row, then
					// send the results
					sendResults();

				// wait until we have another result
				// to send data
			}

			if (BUF_FLUSH) {
				// returns a ctrl flag
				ControlFlag newCtrlFlag = queryInfo.getQueryResult()
						.requestBufFlush();
				if (newCtrlFlag != ControlFlag.NULLFLAG) {
					processCtrlMsg(newCtrlFlag);
				}
			}
			timedOut = true;
			break;
		case SYNCH_PARTIAL:
		case END_PARTIAL:
			// belive I can ignore these two
			break;
		default:
			assert false : "Unexpected control flag " + ctrlFlag.flagName();
		}
	}

	/**
	 * handle a request for more elements This method is valid only for Query
	 * Engine queries It is called whenever the user does getNext
	 * 
	 * @param request
	 *            The request message that came from the client
	 */
	synchronized public void handleRequest(RequestMessage request) {
		this.request = request;
		totalResults += Integer.parseInt(request.requestData);
		// suspended = false;
		notify();
	}

	synchronized public void handleSynchronousRequest() {
		totalResults = Integer.MAX_VALUE;
		// suspended = false;
		notify();
	}

	/**
	 * Suspend this transmitter
	 */
	synchronized public void suspend() {
		if (queryInfo.isAccumFileQuery()) {
			throw new PEException("Can't suspend an Accumulate File Query");
		}
		// suspended = true;
	}

	/**
	 * Resume suspended transmission
	 */
	synchronized public void resume() {
		// suspended = false;
		notify();
	}

	/**
	 * Check whether the transmission should be suspended for any reason
	 */
	// synchronized private boolean checkSuspension() {
	// if (suspended)
	// return true;
	// if (totalResults <= 0) {
	// return true;
	// }
	// return false;
	// }

	/**
	 * Extract the XML string from the result object
	 */
	// private String getResultData(QueryResult.ResultObject ro,
	// boolean prettyprint) {
	// return XMLUtils.flatten(ro.result, prettyprint);
	// }

	// send the results collected so far
	private void sendResults() throws IOException, InterruptedException {

		if (response.dataSize() != 0) {
			handler.sendResponse(response);
		}
		response.clearData();
		resultsSoFar = 0;

	}

	public String getName() {
		return "ResultTransmitter:" + queryInfo.getQueryId();
	}

	public void getInstrumentationValues(HashMap<String, Object> hm) {
		; // No instrumentation for now
	}

	public void getInstrumentationDescriptions(HashMap<String, String> hm) {
		; // No instrumentation for now
	}

	public void setInstrumented(boolean instrumented) {
		; // No instrumentation for now
	}

	public void setHandler(RequestHandler handler) {
		synchronized (intermittentSynch) {
			this.handler = handler;
		}
	}

	public RequestHandler getHandler() {
		synchronized (intermittentSynch) {
			return this.handler;
		}
	}

	/*
	 * private void closeEpoch() throws IOException { sendEndResult();
	 * synchronized(intermittentSynch) {
	 * handler.closeIntermittentConnection(queryInfo.getQueryId()); handler =
	 * null; } System.err.println("XXX vpapad: Done sending an epoch");
	 * //synchronized(queryDone) { // queryDone.notifyAll(); //} }
	 */

	private void sendEndResult() throws IOException {
		handler.sendResponse(new ResponseMessage(request,
				ResponseMessage.END_RESULT));
	}

	public ResponseMessage getMostRecentEpoch() {
		try {
			synchronized (epochQueue) {
				while (epochQueue.size() == 0) {
					epochQueue.wait();
				}
				ResponseMessage resp = (ResponseMessage) epochQueue
						.get(epochQueue.size() - 1);
				epochQueue.clear();
				resultsSoFar = 0;
				return resp;
			}
		} catch (InterruptedException e) {
			System.err.println("Interrupted in getMostRecentEpoch");
			return null;
		}
	}

	/*
	 * public void clearEpochQ () { synchronized (epochQueue) { if
	 * (epochQueue.size() != 0) { epochQueue.clear(); resultsSoFar = 0; } } }
	 */

	private void appendToEpochQ(ResponseMessage response) {
		synchronized (epochQueue) {
			epochQueue.add(response);
			epochQueue.notifyAll();
		}
	}

}
