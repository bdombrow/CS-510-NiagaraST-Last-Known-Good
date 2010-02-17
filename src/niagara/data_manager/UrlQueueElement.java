package niagara.data_manager;

class UrlQueueElement {
	private String url;
	// private String fn;
	private MemCacheEntry me;

	UrlQueueElement(String url, MemCacheEntry me) {
		this.url = url;
		this.me = me;
	}

	public String getUrl() {
		return url;
	}

	public MemCacheEntry getMemCacheEntry() {
		return me;
	}
}
