package niagara.client;

public class SEQuery extends Query {
    String text;

    public SEQuery(String text) {
	this.text = text;
    }

    public String getText() {
	return text;
    }
    
    public String getCommand() {
	return "EXECUTE_SE_QUERY";
    }

    public String getDescription() {
	return "SE";
    }
    public int getType() {
	return QueryType.SEQL;
    }
}

