package niagara.client;

public class QPQuery extends Query {
    String text;

    public QPQuery(String text) {
	this.text = text;
    }

    public String getText() {
	return text;
    }
    
    public String getCommand() {
	return "execute_qp_query";
    }

    public String getDescription() {
	return "QP";
    }
    
    public int getType() {
	return QueryType.QP;
    }
}

