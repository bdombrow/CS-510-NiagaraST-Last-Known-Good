package niagara.client.qpclient;


public class Dup extends Operator {
    public Dup() {
	super();
	type = "dup";
    }
    public void defaultSetLines() {
	lines = new LineDescription[] {new LineDescription("replicate", CENTER, null)};
    }
}
