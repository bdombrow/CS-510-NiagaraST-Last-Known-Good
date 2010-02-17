package niagara.utils;

/** 
 * ControlFlag is an enumeration of control messages which are passed
 * up and downstream during query processing.
 * 
 *  Stream direction (downstream):        Producer -> Consumer
 *  Counter-stream direction (upstream):  Producer <- Consumer
 * 
 * NULLFLAG - just a value to indicate no flag 
 * SHUTDOWN - shutdown is irregular shutdown - due to operator error
 *            or client request. When any operator detects a shutdown
 *            it sends shutdown messages to all of its sink and
 *            source operators
 * GET_PARTIAL - get partial is initiated by the top of the query tree
 *               or by the client and travels down to the bottom
 *               of the operator tree. At the bottom of the tree, when
 *               a get partial is received, a SYNCH_PARTIAL message
 *               is sent up the tree
 * SYNCH_PARTIAL - synch partial messages are sent by non-blocking
 *                 operators to indicate they have synchronized
 *                 on the current partial result request - that is
 *                 they have received either END_PARTIAL or SYNCH_PARTIAL
 *                 messages from all of their source streams and have
 *                 processed all tuples appearing in the stream before
 *                 the SYNCH_PARTIAL and END_PARTIAL messages and
 *                 output any corresponding results. SYNCH_PARTIAL
 *                 is like an END_PARTIAL for non-blocking operators
 * END_PARTIAL - END_PARTIAL messages are output by blocking operators
 *               to indicate that they have output all tuples in the
 *               current partial result request.
 * EOS - EOS is used for normal shutdown due to end of stream. When
 *       an operator completes processing, it puts an EOS into the
 *       stream to indicate it is done.
 * REQUEST_BUF_FLUSH This is sent by a consumer to a producer when
 *       the consumer times out on the producer's stream many times.
 *       It requests that the producer flush its possibly partially-full 
 *       buffer. This is for the case where the producer is producing
 *       tuples slowly - to make sure we still get data in this case.
 *
 * @author rfernand
 * @version 2.0
 *
 */
public enum ControlFlag {
	NULLFLAG("NullFlag"), 
	SHUTDOWN("Shutdown"),                    // upstream & downstream
	GET_PARTIAL("GetPartial"),               // upstream only
	SYNCH_PARTIAL("SynchPartial"),           // downstream only
	END_PARTIAL("EndPartial"),               // downstream only
	EOS("EndOfStream"),                      // downstream only
	REQUEST_BUF_FLUSH("RequestBufferFlush"), // upstream only
	TIMED_OUT("TimedOut"),                   // for returning status only
	CHANGE_QUERY("ChangeQuery"),
	READY_TO_FINISH("ReadyToFinish"),
	IMPUTE("Impute"),                        // RJFM for imputation prototype (ambiguous; PunctQC)
	MESSAGE("Message");                      // RJFM Assumed punctuation prototype (upstream only)

	private final String _flagName;

	ControlFlag(String flagName) {
		this._flagName = flagName;
	}

	/***
	 * 
	 * @return Name of the control flag.
	 */
	public String flagName() {
		return _flagName;
	}

}
