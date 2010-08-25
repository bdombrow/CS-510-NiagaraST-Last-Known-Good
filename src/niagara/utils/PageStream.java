package niagara.utils;

import java.util.ArrayList;

import niagara.connection_server.NiagraServer;

/***
 * Note on removal of upStreamContrlQueue (now toConsumerQueue) and priority put
 * instead of priority put, we have PageStream check the incoming pages to
 * check for any "priority flags" and set a flag in PageStream to indicate
 * that a priority ctrl flag has been received (currently the only priority
 * flag is shutdown) this works to propagate a priority element in all
 * cases except when queue is full, but I think we can live with that. we
 * waste at most one page of processing wait priority propagation could be
 * done by simply setting a flag on page stream... but this won't work if we
 * move to real streams...
 */
@SuppressWarnings("unchecked")
public class PageStream {
	// Buffer for propagating tuples and control elements to Consumer
	private PageQueue toConsumerQueue; // upStreamQueue;

	// Buffer for propagating control information down stream
	// Note, think I always need to be able to put in the toProducer
	// queue without blocking - so make this infinitely expanding (that
	// won't happen of course)
	private PageQueue toProducerQueue; // downStreamQueue;

	// eos indicates end of stream received from producer operator
	private boolean eos;

	// shutdown flag is true if a shutdown control page has been
	// received either from upstream or downstream, shutdown indicates
	// client shutdown request or operator error
	private boolean shutdown;
	private String shutdownMsg;

	private final int STREAM_CAPACITY = 5; // Number of inter-operator pages of tuples
	public static int MAX_DELAY = 1000;

	// keep extra pages around that can be reused
	private TuplePage[] dataPageBuffer;
	private TuplePage extraCtrlPage;

	// for debugging identification
	private String name;

	// instrumentation code
	private int timeouts;
	// private int existingDataPagesUsed;
	// private int dataPagesAllocd;
	// private int existingCtrlPagesUsed;
	// private int ctrlPagesAllocd;
	private int notifiedConsumer;
	private int notifiedProducer;
	private int notifiedOnCtrl;

	// if sendImmediate is true, tuples are not buffered,
	// page is sent immediately
	private boolean sendImmediate;

	/*
	 * XXX vpapad: Here's the idea for replacing timeouts with notifications.
	 * There are two threads using this PageStream, Source and Sink. Source and
	 * Sink can expect a PageStream that has something to report to them to
	 * notify them. Source waits on notifyOnSink, Sink waits on notifyOnSource.
	 * Bah. Confusing. In my understanding, there are 4 events that require
	 * notification: - new page from source -> notify sink - eos flag becomes
	 * true -> notify sink - control message from sink -> notify source -
	 * shutdown flag becomes true -> notify the other side
	 * 
	 * To get the notification behavior a thread needs to register itself as a
	 * source or a sink. Otherwise, no notifications are issued (and the thread
	 * needs to use timeouts.)
	 */
	/** Object to notify when the source has put something in */
	private MailboxFlag notifyOnSource;
	/** Object to notify when the sink has put something in */
	private MailboxFlag notifyOnSink;

	/**
	 * Constructor
	 */
	public PageStream(String name) {
		// toProducerQueue is expandable...
		toConsumerQueue = new PageQueue(STREAM_CAPACITY, false, name
				+ "_toCons");
		toProducerQueue = new PageQueue(STREAM_CAPACITY, true, name + "_toProd");
		dataPageBuffer = new TuplePage[STREAM_CAPACITY];
		extraCtrlPage = null;
		eos = false;
		shutdown = false;
		this.name = name;

		if (NiagraServer.DEBUG2) {
			// instrumentation
			timeouts = 0;
			// existingDataPagesUsed = 0;
			// dataPagesAllocd = 0;
			// existingCtrlPagesUsed = 0;
			// ctrlPagesAllocd = 0;
			notifiedConsumer = 0;
			notifiedProducer = 0;
			notifiedOnCtrl = 0;
		}
	}

	public String getName() {
		return name;
	}

	// ----------------------------------------------------------------------
	// FUNCTIONS FOR USE BY SourceTupleStream ONLY
	// ----------------------------------------------------------------------

	/**
	 * Get a page from the source operator (toConsumerQueue). This function
	 * should only be called by the consumer.
	 * 
	 * If no page is received from source within timeout milliseconds, function
	 * times out and null is returned. A timeout of 0, causes the time to be
	 * ignored and function will block until page received from source.
	 * 
	 * EOS is indicated by a flag on the page.
	 * 
	 * @param timeout
	 *            A timeout period in milliseconds
	 * 
	 * @return Page from the source (toConsumerQueue), page may have tuples, a
	 *         control flag, or both, null if timed out
	 * 
	 */
	// to be called by SourceTupleStream ONLY
	public TuplePage getPageFromSource(int timeout)
			throws InterruptedException, ShutdownException {
		return consumerGetPage(timeout);
	}

	private synchronized TuplePage consumerGetPage(int timeout)
			throws InterruptedException, ShutdownException {

		if (shutdown)
			throw new ShutdownException(shutdownMsg);

		// note, get is allowed after end of stream

		// If the buffer is empty, wait the timeout period, if I
		// wake up and the buffer is still empty, means I timed out

		// Note - only one consumer on this stream. Notifies come
		// from two possibilites - consumer notifies on toProducerQueue
		// or producer notifies on toConsumerQueue, since consumer (which
		// is the only one to call this function) can't notify when
		// sleeping and producer notifies only on toConsumerQueue,
		// if we wake up from wait one of two things has happened
		// 1) timeout
		// 2) producer notified on toConsumerQueue.
		// we can test which of these two situations we are in by
		// checking if toConsumerQueue is empty - notice that since
		// there are no other consumers on this queue, no one else
		// could have taken elements out of this queue before consumer
		// got to run

		if (toConsumerQueue.isEmpty()) {
			if (timeout > 0) {
				wait(timeout);
				if (toConsumerQueue.isEmpty()) {
					// I timed out...
					if (NiagraServer.DEBUG2)
						timeouts++;
					return null;
				}
			} else {
				// caller does not want to wait on this stream
				return null;
			}
			// else must be something in queue, go on
		}

		if (shutdown)
			throw new ShutdownException(shutdownMsg);

		// we have something in the queue...
		// don't check for flags because 1) if shutdown flag, will
		// already have been checked and we shouldn't get here,
		// 2) other flags appear logically after all tuples in page
		boolean notifyProducer = toConsumerQueue.isFull();
		TuplePage ret = toConsumerQueue.get();

		// KT - DEL after functions - just for checking
		assert ret.getFlag() != ControlFlag.SHUTDOWN : "KT shouldn't get here. Thread: "
				+ Thread.currentThread().getName() + " Stream name: " + name;

		if (notifyProducer) {
			if (NiagraServer.DEBUG2)
				notifiedProducer++;
			notify();
		}

		return ret;
	}

	/**
	 * Put a controlPage to the source (toProducer queue). This function is
	 * non-blocking because the toProducer queue is "infinitely" expandable,
	 * although it should never grow big at all.
	 * 
	 * @param controlPage
	 *            the control page to be sent
	 * 
	 */
	// To be called by SourceTupleStream ONLY
	// just a wrapper with a better name for calling program
	// REFACTOR
	public ControlFlag putCtrlMsgToSource(ControlFlag ctrlMsgId,
			String ctrlMsgStr, FeedbackPunctuation fp) throws ShutdownException {
		return consumerPutCtrlMsg(ctrlMsgId, ctrlMsgStr, fp);

	}

	private synchronized ControlFlag consumerPutCtrlMsg(ControlFlag ctrlMsgId,
			String ctrlMsgStr, FeedbackPunctuation fp) throws ShutdownException {

		if (eos){
			//System.out.println(this.getName() + " Try to put msg when source closed");
			return ControlFlag.EOS;
		} else
		{
		//System.out.println(this.getName()+" Putting control message when not closed");
		
		}
		if (shutdown) {
		//System.out.println(this.getName() + " Throw shutdown");
			throw new ShutdownException(shutdownMsg);
		}

		// do SHUTDOWN check on put to make propagation of SHUTDOWN
		// as fast as possible
		if (ctrlMsgId == ControlFlag.SHUTDOWN) {
			shutdown = true;
			shutdownMsg = ctrlMsgStr;
		}

		boolean notify = toProducerQueue.isEmpty();

		// Add the control element to the end of the down stream control
		// buffer
		// System.out.println("KT: Thread" +
		// Thread.currentThread().getName() +
		// " Putting to producer queue " + name + "  " +
		// CtrlFlags.name[ctrlMsgId]);
		toProducerQueue.put(getCtrlPage(ctrlMsgId, ctrlMsgStr, fp));

		if (notify) {
			if (NiagraServer.DEBUG2)
				notifiedOnCtrl++;
			notify();
		}

		notifySource();

		return ControlFlag.NULLFLAG; // indicates success
	}

	// to be called by SourceTupleStream ONLY
	public synchronized boolean shutdownReceived() {
		return shutdown;
	}

	public synchronized String getShutdownMsg() {
		return shutdownMsg;
	}

	// to be called by SourceTupleStream ONLY
	public synchronized void returnTuplePage(TuplePage returnPage) {
		for (int i = 0; i < STREAM_CAPACITY; i++) {
			if (dataPageBuffer[i] == null) {
				dataPageBuffer[i] = returnPage;
				return;
			}
		}
		// if buffer is full, toss the return page
		return;
	}

	// ----------------------------------------------------------------------
	// FUNCTIONS FOR USE BY SinkTupleStream ONLY
	// ----------------------------------------------------------------------
	/**
	 * This function returns a control flag sent from the sink operator (the
	 * operator "above" this operator in the query tree) a.k.a. a page from the
	 * toProducerQueue, if any exists. Otherwise, it returns NULLFLAG. This
	 * function is non-blocking.
	 * 
	 * @return The control flag received from the sink message, NULLFLAG if no
	 *         control element received.
	 * 
	 */
	// To be called by SinkTupleStream ONLY
	// just a wrapper with a better name...
	// public int getCtrlMsgFromSink() throws ShutdownException {
	// return producerGetCtrlMsg();
	// }

	public ArrayList getCtrlMsgFromSink(int timeout)
			throws InterruptedException, ShutdownException {
		return producerGetCtrlMsg(timeout);
	}

	private synchronized ArrayList producerGetCtrlMsg(int timeout)
			throws InterruptedException, ShutdownException {
		// shutdown takes priority over all other messages
		if (shutdown) {
			throw new ShutdownException(shutdownMsg);
		}

		// Check for end of stream and raise exception if necessary
		assert !eos : "KT Reading after end of stream";

		// Get first element, if any, in the down stream buffer
		if (timeout >= 0)
			// XXX vpapad I think there's a potential bug here.
			// In consumerGetPage() timeout=0 means don't wait.
			// Here, timeout=0 calls wait(0), which means wait forever.
			// One needs to call this function with timeout=-1 to get the
			// non-blocking behavior!
			while (toProducerQueue.isEmpty()) {
				wait(timeout);
			}
		else {
			if (toProducerQueue.isEmpty())
				return null;
			//else wait(1000);
		}

		// !toProducerQueue.isEmpty() must be false;
		// no notification, no one ever blocks on toProducerQueue
		TuplePage ctrlPage = toProducerQueue.get();
		ArrayList ctrl = null;
		ControlFlag ctrlFlag = ctrlPage.getFlag();
		if (ctrlFlag == ControlFlag.MESSAGE) { // RJFM
			FeedbackPunctuation fp = ctrlPage.getFeedbackPunctuation();
			ctrl = new ArrayList(3);
			ctrl.add(ctrlFlag);
			ctrl.add(ctrlPage.getCtrlMsg());
			ctrl.add(fp);
		}
		returnCtrlPage(ctrlPage); // make page avail for reuse
		return ctrl;
	}

	/**
	 * Put a page to the Sink operator - a.k.a. put a page in the
	 * toConsumerQueue.
	 * 
	 * This function checks for control pages from the sink. If there is a
	 * control page(message) from the sink, the tuplePage is not put in the
	 * stream and the control page is returned. This function blocks until
	 * either the output element can be put in the toConsumerQueue (to Sink) or
	 * a control page is read from the toProducerQueue (from Sink) (this is our
	 * flow control mechanism)
	 * 
	 * @param page
	 *            The page to be sent to the Sink
	 * 
	 * @return the control flag, NULLFLAG if put was successful
	 * 
	 */
	// to be called by SinkTupleStream ONLY
	public ArrayList putPageToSink(TuplePage page) throws InterruptedException,
			ShutdownException {
		return producerPutPage(page);
	}

	private synchronized ArrayList producerPutPage(TuplePage page)
			throws InterruptedException, ShutdownException {

		// shutdown takes priority over everything
		if (shutdown)
			throw new ShutdownException(shutdownMsg);

		assert !eos : "KT Writing after end of stream";

		// Wait until either the up stream tuple buffer is not full
		// (so that the outputElement can be put) or the down stream
		// control buffer is not empty (so that a control element can
		// be returned).
		while (toConsumerQueue.isFull() && toProducerQueue.isEmpty()) {
			// note - I think that if I get a control message from
			// "below" that is if the producer gets a control message
			// from the operator below it in the control tree, it
			// will not wake up
			//notifyAll();
			wait();
			//wait(1000);
		}

		// If there is a control element in the down stream buffer,
		// then return that
		if (toProducerQueue.isNonEmpty()) {
			TuplePage ctrlPage = toProducerQueue.get();

			String ctrlMsg = ctrlPage.getCtrlMsg();
			ControlFlag ctrlFlag = ctrlPage.getFlag();
			if (ctrlFlag == ControlFlag.SHUTDOWN) {
				assert shutdown : "@*#$!#*$";
				returnCtrlPage(ctrlPage);
				throw new ShutdownException(ctrlMsg);
			} else {
				if (ctrlFlag == ControlFlag.MESSAGE) { // RJFM
					returnCtrlPage(ctrlPage);
					FeedbackPunctuation fp = ctrlPage.getFeedbackPunctuation();
					ArrayList ret = new ArrayList(3);
					ret.add(ctrlFlag);
					ret.add(ctrlPage.getCtrlMsg());
					ret.add(fp);
					return ret;
				} else {
				returnCtrlPage(ctrlPage);
				ArrayList ret = new ArrayList(2);
				ret.add(ctrlFlag);
				ret.add(ctrlMsg);
				return ret;
				}				
			}
		} else {
			// do SHUTDOWN check on put to make propagation of SHUTDOWN
			// as fast as possible - you may find it strange to
			// check shutdown in put - this check is done not for
			// the producer, but for the consumer - note that this shutdown
			// message is just arriving into the consumers
			// input stream
			if (page.getFlag() == ControlFlag.SHUTDOWN) {
				shutdown = true;
				shutdownMsg = page.getCtrlMsg();
				// System.out.println("KT: Thread " +
				// Thread.currentThread().getName() +
				// " Putting to consumer queue "
				// + name + "  " +
				// CtrlFlags.name[page.getFlag()]);
			}

			// There must be an open spot in the toConsumerQueue, so put
			// the page there.
			boolean notifyConsumer = toConsumerQueue.isEmpty();
			toConsumerQueue.put(page);

			if (notifyConsumer) {
				if (NiagraServer.DEBUG2)
					notifiedConsumer++;
				notify();
			}

			notifySink();

			return null; // indicates successful put
		}
	}

	/**
	 * This function closes a stream so that no further upward or downward
	 * communication (other than get) is possible. This function blocks until
	 * there is space available in the toConsumerQueue.
	 * 
	 */
	// To be called by SinkTupleStream ONLY
	public synchronized void endOfStream() {
		// If the stream was previously closed, throw an exception
		assert !eos : "KT end of stream received twice";
		eos = true;
        //System.out.println(this.getName() + " set EOS");
		notifySource();
        //System.out.println(this.getName() + " source has been served!");
	
		if (NiagraServer.DEBUG2) {
			System.out.println("PageStream: " + name + " results.");
			System.out.println("   Timeouts: " + timeouts);
			// System.out.println("Existing Data Pages Used "
			// +existingDataPagesUsed);
			// System.out.println("Data Pages Allocd " + dataPagesAllocd);
			// System.out.println("Existing Ctrl Pages Used "
			// +existingCtrlPagesUsed);
			// System.out.println("Ctrl Pages Used " + ctrlPagesAllocd);
			// System.out.println(name);
			System.out.println("   Notified Consumer: " + notifiedConsumer);
			System.out.println("   Notified Producer " + notifiedProducer);
			System.out.println("   Notified on Control " + notifiedOnCtrl);
			// System.out.println();
		}
	}

	// to be called by SinkTupleStream ONLY
	public synchronized TuplePage getTuplePage() {
		TuplePage ret;
		for (int i = 0; i < STREAM_CAPACITY; i++) {
			if (dataPageBuffer[i] != null) {
				ret = dataPageBuffer[i];
				dataPageBuffer[i] = null;
				// existingDataPagesUsed++;
				return ret;
			}
		}
		// dataPagesAllocd++;
		return new TuplePage();
	}

	private TuplePage getCtrlPage(ControlFlag ctrlMsgId, String ctrlMsgStr, FeedbackPunctuation fp) {
		if (extraCtrlPage != null) {
			TuplePage ret = extraCtrlPage;
			extraCtrlPage = null;
			ret.setFlag(ctrlMsgId);
			ret.setCtrlMsg(ctrlMsgStr);
			ret.setFeedbackPunctuation(fp);
			// existingCtrlPagesUsed++;
			return ret;
		} else {
			// ctrlPagesAllocd++;
			return TuplePage.createControlPage(ctrlMsgId, ctrlMsgStr, fp);
		}
	}

	private void returnCtrlPage(TuplePage returnPage) {
		if (extraCtrlPage != null) {
			if (NiagraServer.DEBUG) {
				System.out.println("KT dropping extra control page");
				System.out.println("KT ".toString());
			}
		}
		returnPage.setFlag(ControlFlag.NULLFLAG);
		extraCtrlPage = returnPage;
	}

	/**
	 * Return a string representation of this stream
	 * 
	 * @return the string representation of this stream
	 */
	public synchronized String toString() {
		String retStr = new String("\nTo Consumer Tuple/Control Queue\n");
		retStr += toConsumerQueue.toString();
		retStr += "\n To Producer Control Queue\n";
		retStr += toProducerQueue.toString();
		retStr += "\n eos: " + eos + " shutdown: " + shutdown + " "
				+ shutdownMsg + "\n";
		return retStr;
	}

	public boolean isSendImmediate() {
		return sendImmediate;
	}

	public void setSendImmediate(boolean sendImmediate) {
		this.sendImmediate = sendImmediate;
	}

	private void notifySink() {
		if (notifyOnSource != null)
			notifyOnSource.raise();
	}

	private void notifySource() {
		if (notifyOnSink != null)
			notifyOnSink.raise();
	}

	public void setNotifyOnSink(MailboxFlag notifyOnSink) {
		this.notifyOnSink = notifyOnSink;
	}

	public void setNotifyOnSource(MailboxFlag notifyOnSource) {
		this.notifyOnSource = notifyOnSource;
	}

}
