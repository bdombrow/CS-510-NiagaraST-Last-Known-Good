/* $Id: State.java,v 1.1 2003/10/01 04:42:21 vpapad Exp $ */
package niagara.logical.path;

public class State {
    public int id;
    public boolean accepting;

    public State(int id, boolean accepting) {
        this.id = id;
        this.accepting = accepting;
    }
}