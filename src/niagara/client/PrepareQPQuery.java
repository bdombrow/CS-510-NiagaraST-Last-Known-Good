package niagara.client;

public class PrepareQPQuery extends Query {
	String text;

	public PrepareQPQuery(String text) {
		this.text = text;
	}

	public String getText() {
		return text;
	}

	public String getCommand() {
		return "prepare_query";
	}

	public String getDescription() {
		return "PrepareQP";
	}

	public int getType() {
		return QueryType.PREPARE;
	}
}
