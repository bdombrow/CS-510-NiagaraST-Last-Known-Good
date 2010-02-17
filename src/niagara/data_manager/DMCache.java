package niagara.data_manager;

/**
 * Niagara DataManager a Universal interface to Cache
 */

interface DMCache {
	public Object fetch(Object key, Object me) throws CacheFetchException;

	public Object fetch_reload(Object key, Object me)
			throws CacheFetchException;

	public void release() throws CacheReleaseException;

	public void pin(Object key) throws CachePinException;

	public void unpin(Object key) throws CacheUnpinException;
}
