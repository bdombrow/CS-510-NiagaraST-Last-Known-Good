/* $Id: KillFooPreparedQPQuery.java,v 1.1 2007/05/19 15:04:54 tufte Exp $ */
package niagara.client;

public class KillFooPreparedQPQuery extends Query {
    String text;

    public KillFooPreparedQPQuery(String text) {
    this.text = text;
    }

    public String getText() {
    return text;
    }
    
    public String getCommand() {
    return "kill_query";
    }

    public String getDescription() {
    return "KillPrepared";
    }
    
    public int getType() {
    return QueryType.KILL_FOOBAR;
    }
}
