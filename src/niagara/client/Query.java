package niagara.client;

abstract public class Query {
    String text;

    public String getText() {
	return text;
    }
    
    abstract public String getCommand();
    abstract public String getDescription();
    abstract public int getType();
}

