package niagara.query_engine;

/**
 * Exception thrown when there are no data source for running the query i.e. SE
 * has returned no URLs
 */
@SuppressWarnings("serial")
public class NoDataSourceException extends Exception {
	public NoDataSourceException() {
		super();
	}

	public NoDataSourceException(String msg) {
		super(msg);
	}
}
