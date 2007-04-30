/* $Id: PrepareQPQuery.java,v 1.1 2007/04/30 19:15:28 vpapad Exp $ */
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
    return QueryType.PREPARE_QP;
    }
}
