package niagara.client;

public class XMLQLQuery extends Query {
    String text;

    public XMLQLQuery(String text) {
	this.text = text;
    }

    public String getText() {
	return text;
    }
    
    public String getCommand() {
	return "execute_qe_query";
    }
    
    public String getDescription() {
	return "XMLQL";
    }

    public int getType() { 
	return QueryType.XMLQL;
    }
}

