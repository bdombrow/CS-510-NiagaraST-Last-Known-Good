/* $Id: SynchronousQPQuery.java,v 1.1 2002/09/14 04:54:14 vpapad Exp $ */
package niagara.client;

public class SynchronousQPQuery extends Query {
    String text;

    public SynchronousQPQuery(String text) {
    this.text = text;
    }

    public String getText() {
    return text;
    }
    
    public String getCommand() {
    return "synchronous_qp_query";
    }

    public String getDescription() {
    return "SynchronousQP";
    }
    
    public int getType() {
    return QueryType.SYNCHRONOUS_QP;
    }
}