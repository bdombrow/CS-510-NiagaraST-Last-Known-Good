/* $Id$ */
package niagara.client;

public class MQPQuery extends Query {
    String text;

    public MQPQuery(String text) {
    this.text = text;
    }

    public String getText() {
    return text;
    }
    
    public String getCommand() {
    return "mqp_query";
    }

    public String getDescription() {
    return "MQP";
    }
    
    public int getType() {
    return QueryType.MQP;
    }
}
