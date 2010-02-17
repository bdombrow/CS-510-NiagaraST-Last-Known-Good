package niagara.logical.path;

public class State {
	public int id;
	public boolean accepting;

	public State(int id, boolean accepting) {
		this.id = id;
		this.accepting = accepting;
	}
}