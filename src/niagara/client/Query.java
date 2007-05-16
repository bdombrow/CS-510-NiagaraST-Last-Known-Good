package niagara.client;

abstract public class Query {
    String text;
    boolean intermittent;

    public String getText() {
    	return text;
    }

    public void setIntermittent(boolean intermittent) {
    	this.intermittent = intermittent;
    }

    public boolean isIntermittent() {
    	return intermittent;
    }
    
    abstract public String getCommand();
    abstract public String getDescription();
    abstract public int getType();
}

