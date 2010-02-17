package niagara.utils;

/**
 * A TuplePage is (surprise) a page of tuple objects. TuplePages are the unit of
 * transfer between operators. Tuple pages contain an array of tuple objects as
 * well as flags such as shutdown, get partial results and so on.
 * 
 * Note this is a buffer, not a queue, interleaving calls to get and put is not
 * recommended.
 * 
 * @version 1.0
 */

public class TuplePage {

	private Tuple tuples[];

	// number of tuples in a page
	// For CIDR: apparently a multiple of detectors per segment * segments + 1
	// (for puncts.)
	// 361
	private final static int PAGE_SIZE = 1;

	// pointer to the first open position in the tuple array
	// tuples are filled in sequentially starting at position 0
	int currentPos;
	int bufSize;

	private ControlFlag flag;
	private String ctrlMsg;

	// the page has "getMode" for when you are reading from the page
	// and "putMode" for filling the page - this flag keeps track of
	// which mode we are in and is really for safety purposes only
	// true means getMode, false means putMode
	private boolean getMode;

	/**
	 * Pages are always a fixed size.
	 * 
	 */

	public TuplePage() {
		this(false);
	}

	/**
	 * This constructor creates an empty page to be used for control elements
	 * going down stream. It will only be called from createControlPage
	 * function.
	 */
	private TuplePage(boolean empty) {
		if (empty)
			tuples = null;
		else {
			tuples = new Tuple[PAGE_SIZE];
		}
		currentPos = 0;
		bufSize = 0;
		getMode = false;
		flag = ControlFlag.NULLFLAG;
	}

	// REFACTOR
	public static TuplePage createControlPage(ControlFlag controlMsgId,
			String ctrlMsgStr) {
		TuplePage ret = new TuplePage(true);
		ret.setFlag(controlMsgId);
		ret.ctrlMsg = ctrlMsgStr;
		return ret;
	}

	public void setFlag(ControlFlag flag) {
		this.flag = flag;
	}

	public void setCtrlMsg(String ctrlMsg) {
		this.ctrlMsg = ctrlMsg;
	}

	public String getCtrlMsg() {
		return ctrlMsg;
	}

	/**
	 * Sets the currentPosition pointer appropriately, must be called before
	 * putting any tuples in the page
	 */
	public void startPutMode() {
		currentPos = 0;
		bufSize = 0;
		getMode = false;
	}

	/**
	 * Put a tuple in the page. Throws an exception if the buffer is full.
	 * 
	 * Note, this is a buffer not a queue. Calls to put and get should NOT be
	 * interleaved. A sequence of put calls should be used to fill the page, a
	 * sequence of get calls can then be used to read from the page.
	 * 
	 */
	public void put(Tuple tuple) {
		assert !getMode : "KT Can't put tuples in getMode";
		assert currentPos < PAGE_SIZE : "KT Reading tuples into a full page";

		tuples[currentPos] = tuple;
		currentPos++;
	}

	/**
	 * Sets the currentPosition pointer appropriately, must be called before
	 * getting any tuples from the page
	 */
	public void startGetMode() {
		bufSize = currentPos;
		currentPos = 0;
		getMode = true;
	}

	/**
	 * Get a tuple from the page. Throws an exception if the buffer is empty.
	 * 
	 * Note, this is a buffer not a queue. Calls to put and get should NOT be
	 * interleaved. A sequence of put calls should be used to fill the page, a
	 * sequence of get calls can then be used to read from the page.
	 * 
	 */
	public Tuple get() {
		assert getMode : "KT Can't get tuples in putMode";
		assert currentPos < bufSize : "KT Reading tuple from empty page";

		Tuple ret = tuples[currentPos];
		tuples[currentPos] = null;
		currentPos++;
		return ret;
	}

	/**
	 * In put mode, is the page full?
	 */
	public boolean isFull() {
		return currentPos == PAGE_SIZE;
	}

	/**
	 * For get mode, have we read all the tuples?
	 */
	public boolean isEmpty() {
		return currentPos == bufSize;
	}

	/***
	 * 
	 * @return ControlFlag
	 */
	public ControlFlag getFlag() {
		return flag;
	}

	public boolean hasTuples() {
		if (tuples == null)
			return true;
		else
			return false;
	}
}
