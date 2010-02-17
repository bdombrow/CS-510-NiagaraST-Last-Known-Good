package niagara.query_engine;

/**
 * Exception thrown when extracting and pushing search engine queries from query
 * engine queries
 */
@SuppressWarnings("serial")
public class PushSEQueryException extends Exception {
	public PushSEQueryException() {
		super();
	}

	public PushSEQueryException(String msg) {
		super(msg);
	}
}
