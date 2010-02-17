package niagara.data_manager;

import niagara.utils.SynchronizedQueue;

class UrlQueue {
	private SynchronizedQueue urlq;

	UrlQueue(int capacity) {
		super();
		urlq = new SynchronizedQueue(capacity);
	}

	public void addUrl(UrlQueueElement url) {
		urlq.put(url, true);
	}

	public UrlQueueElement getUrl() {
		return ((UrlQueueElement) urlq.get());
	}
}
