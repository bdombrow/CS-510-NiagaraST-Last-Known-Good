package niagara.client;

public class SpecialFunctionQuery extends Query {
    String text;

    public SpecialFunctionQuery(String text) {
	this.text = text;
    }

    public String getText() {
	return text;
    }
    
    public String getCommand() {
	return text;
    }

    public String getDescription() {
	return text;
    }
    
    public int getType() {
	return QueryType.NOTYPE;
    }
}

