package niagara.data_manager;

/***
 * Niagara DataManager a Universal interface to Cache
 */
@SuppressWarnings("serial")
class CachePinException extends DMException {
	public CachePinException() {
		super("Cache Pin Exp: ");
	}

	public CachePinException(String msg) {
		super("Cache Pin Exp: " + msg);
	}
}
