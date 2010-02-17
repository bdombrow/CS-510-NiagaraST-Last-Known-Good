package niagara.client;

public class ExecutePreparedQPQuery extends Query {
	String text;

	public ExecutePreparedQPQuery(String text) {
		this.text = text;
	}

	public String getText() {
		return text;
	}

	public String getCommand() {
		return "execute_prepared_query";
	}

	public String getDescription() {
		return "ExecutePrepared";
	}

	public int getType() {
		return QueryType.EXECUTE_PREPARED;
	}
}
