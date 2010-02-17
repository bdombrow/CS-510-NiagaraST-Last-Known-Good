package niagara.data_manager;

/***
 * Niagara DataManager a Universal interface to Cache
 */
@SuppressWarnings("serial")
class CacheReleaseException extends DMException {
	public CacheReleaseException() {
		super("Cache Release Exp: ");
	}

	public CacheReleaseException(String msg) {
		super("Cache Release Exp: " + msg);
	}
}
