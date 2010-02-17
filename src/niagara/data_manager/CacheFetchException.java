package niagara.data_manager;

/***
 * Niagara DataManager a Universal interface to Cache
 */
@SuppressWarnings("serial")
class CacheFetchException extends DMException {
	public CacheFetchException() {
		super("Cache Fetch Exp: ");
	}

	public CacheFetchException(String msg) {
		super("Cache Fetch Exp: " + msg);
	}
}
