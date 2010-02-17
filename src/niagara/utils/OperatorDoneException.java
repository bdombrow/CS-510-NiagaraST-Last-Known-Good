package niagara.utils;

/**
 * OperatorDoneException is thrown when an operator is done. This does not cause
 * an error message to be propagated to the client, rather operators below this
 * operator are shutdown and this operators output streams are closed
 * 
 * @version 1.0
 * 
 */

@SuppressWarnings("serial")
public class OperatorDoneException extends Exception {

	public OperatorDoneException() {
	}

}
