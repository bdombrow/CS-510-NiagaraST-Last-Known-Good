package niagara.data_manager;

import java.util.Vector;

import niagara.utils.SinkTupleStream;

@SuppressWarnings("unchecked")
class FetchRequest {
	SinkTupleStream s;
	Vector urls;
	Vector paths;

	FetchRequest(SinkTupleStream s, Vector urls, Vector paths) {
		this.s = s;
		this.urls = urls;
		this.paths = paths;
	}
}
