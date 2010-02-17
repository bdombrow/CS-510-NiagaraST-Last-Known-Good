package niagara.client;

/**
 * This interface is implemented by the GUI
 * 
 */

public interface UIDriverIF {

	/**
	 * Notifies the UI that new results for query have arrived
	 * 
	 * @param query
	 *            id
	 */
	public void notifyNew(int id);

	/**
	 * Notifies the UI that all the results for query have arrived
	 * 
	 * @param query
	 *            id
	 */
	public void notifyFinalResult(int id);

	/**
	 * Displays error messages
	 */
	public void errorMessage(int id, String err);
}
