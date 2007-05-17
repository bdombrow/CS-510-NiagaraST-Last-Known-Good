/* $Id: PrepareQPQuery.java,v 1.2 2007/05/17 21:13:22 tufte Exp $ */
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
