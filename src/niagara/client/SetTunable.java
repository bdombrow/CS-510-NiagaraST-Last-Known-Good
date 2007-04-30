/* $Id: SetTunable.java,v 1.1 2007/04/30 19:15:29 vpapad Exp $ */
package niagara.client;

public class SetTunable extends Query {
    String text;

    public SetTunable(String text) {
    this.text = text;
    }

    public String getText() {
    return text;
    }
    
    public String getCommand() {
    return "set_tunable";
    }

    public String getDescription() {
    return "SetTunable";
    }
    
    public int getType() {
    return QueryType.SET_TUNABLE;
    }
}
