package niagara.data_manager;

/***
 * Niagara DataManager a Universal interface to Cache
 */
@SuppressWarnings("serial")
class CacheUnpinException extends DMException {
	public CacheUnpinException() {
		super("Cache Unpin Exp: ");
	}

	public CacheUnpinException(String msg) {
		super("Cache Unpin Exp: " + msg);
	}
}
