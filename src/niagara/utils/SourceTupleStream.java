package niagara.utils;

/**
 * 
 * TupleSourceStream provides a tuple-oriented stream interface for operators to
 * use. Underneath, the tuples are batched into pages and the pages are passed
 * to the next operator using a page stream. This gives operators a
 * tuple-oriented view, like they want yet avoids element-by-element
 * synchronization. This class implements a source stream for reading from - a
 * similar class implements a sink stream.
 * 
 * @version 1.0
 * 
 */

public final class SourceTupleStream {

	private PageStream pageStream;
	private TuplePage buffer;

	// flag of most recent control message received
	private ControlFlag ctrlFlag;
	// private int ctrlFlag;

	int status; // open, closed, synchpartial, etc

	/**
	 * possible statues for source streams
	 * 
	 * Meanings of statuses: Open means this stream is active and we are reading
	 * data from it (think open streams should match active source streams)
	 * SynchronizePartialResult means a SYNCH_PARTIAL message has been received
	 * from that stream EndOfPartial means an END_PARTIAL message has been
	 * received on that stream Closed means operator below has closed this
	 * stream (normal shutdown)
	 * 
	 * END_PARTIAL messages are sent by blocking operators when they finish
	 * outputting a partial result SYNCH_PARTIAL messages are sent by
	 * non-blocking operators to indicate they have received either END_PARTIAL
	 * or SYNCH_PARTIAL messages from all source streams. SYNCH_PARTIAL is like
	 * END_PARTIAL from non-blocking operators. See CtrlFlags.java for more.
	 */
	public static final int Open = 0;
	public static final int SynchPartial = 1;
	public static final int EndPartial = 2;
	public static final int Closed = 3;

	/**
	 * Default Constructor without parameters to allow subclassing
	 */
	public SourceTupleStream(PageStream stream) {
		pageStream = stream;
		buffer = null;
		status = Open;
	}

	public String getName() {
		return pageStream.getName();
	}

	/**
	 * Get a tuple from the stream. If there is a tuple in the buffer, that is
	 * returned first, except if the shutdown flag has been set, then check for
	 * control element at end of page (recall a control flag on a page indicates
	 * a control element of that type appears at the point in the stream after
	 * all tuples on the page), then read from PageStream and return the first
	 * tuple from that page.
	 * 
	 * Function returns a tuple, if found. If timeout or control elt found,
	 * returns null. ctrlFlag set to TIMED_OUT if timeout occurred.
	 * 
	 * If the stream is closed and all the data items have been returned in
	 * previous "gets", then the function returns null and controlFlag is set to
	 * EOS.
	 * 
	 * @param timeout
	 *            number of milliseconds to wait for an element coming from the
	 *            operator below, if timeout milliseconds go past null is
	 *            returned and timedOut flag set to true
	 * 
	 * @return next tuple, or null if timeout or control element detected
	 * 
	 * @exception java.lang.InterruptedException
	 *                The thread is interupped in the middle of execution - KT
	 *                what does this mean???
	 */
	public Tuple getTuple(int timeout) throws InterruptedException,
			ShutdownException {

		ctrlFlag = ControlFlag.NULLFLAG; // for safety

		// get allowed after eos
		if (buffer == null) {
			// buffer is null - means we need to get one from pageStream
			buffer = pageStream.getPageFromSource(timeout);
			// if getPageFromSource returns null, it means it timed out
			if (buffer == null) {
				ctrlFlag = ControlFlag.TIMED_OUT;
				return null;
			}

			// switch buffer flag, so we can read tuples
			buffer.startGetMode();
		} else {
			// check for shutdown - this code here just helps
			// propagate shutdown ASAP
			if (pageStream.shutdownReceived())
				throw new ShutdownException(pageStream.getShutdownMsg());
		}

		if (!buffer.isEmpty()) {
			Tuple retTuple = buffer.get();

			// never leave an empty buffer without a control flag around
			if (buffer.isEmpty() && buffer.getFlag() == ControlFlag.NULLFLAG) {
				pageStream.returnTuplePage(buffer);
				buffer = null;
			}
			return retTuple;
		} else {
			// cases: 1) previous getNextTuple emptied buffer,
			// but there is a control flag to be processed
			// 2) we just received empty page with control flag
			// 3) we received totally empty page (no data, no control)
			if (buffer.getFlag() != ControlFlag.NULLFLAG) {
				// handle cases 1 and 2 by returning control flag
				ctrlFlag = buffer.getFlag();
				pageStream.returnTuplePage(buffer);
				buffer = null;
				return null;
			} else {
				assert false : "KT detected page with no data and no control";
				return null;
			}
		}
	}

	/**
	 * This function puts a control element down stream. This function is
	 * non-blocking.
	 * 
	 * @param controlElement
	 *            the control element to be sent
	 * 
	 * @return returns CtrlFlags.NULLFLAG on success, control flag otherwise
	 */
	// REFACTOR
	public ControlFlag putCtrlMsg(ControlFlag ctrlMsgId, String ctrlMsg)
			throws ShutdownException {
		return pageStream.putCtrlMsgToSource(ctrlMsgId, ctrlMsg);
	}

	/**
	 * Returns the controlFlag detected during previous getNextTuple call
	 */
	public ControlFlag getCtrlFlag() {
		return ctrlFlag;
	}

	/*
	 * public int getCtrlFlag() { return ctrlFlag; }
	 */

	/**
	 * Return a string representation of this stream
	 * 
	 * @return the string representation of this stream
	 */
	public String toString() {
		String retStr = new String("\nTuple Buffer\n");
		retStr += buffer.toString();
		retStr += "\nstatus: " + status + " ctrlFlag " + ctrlFlag.flagName()
				+ "\n";
		return retStr;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int newStatus) {
		status = newStatus;
	}

	public boolean isClosed() {
		return status == Closed;
	}

	public boolean isSendImmediate() {
		return pageStream.isSendImmediate();
	}

	public void setNotifyOnSource(MailboxFlag notifyMe) {
		pageStream.setNotifyOnSource(notifyMe);
	}
}
