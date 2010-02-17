package niagara.logical.path;

class Transition {
	State from;
	String label;
	State to;

	public Transition(State from, String label, State to) {
		this.from = from;
		this.label = label;
		this.to = to;
	}

	// special constants
	static String epsilon = new String("epsilon");
	static String wildcard = new String("*");
}