package niagara.client;

public class TriggerQuery extends Query {
    String text;

    public TriggerQuery(String text) {
	this.text = text;
    }

    public String getText() {
	return text;
    }
    
    public String getCommand() {
	return "execute_trigger_query";
    }
    
    public String getDescription() {
	return "Trigger";
    }

    public int getType() {
	return QueryType.TRIG;
    }
}

