package niagara.xmlql_parser.op_tree;

/**
 * StreamSpec - base class for stream specification -
 * extending classes: FileScanSpec, FirehoseScanSpec
 */

public class StreamSpec {
    protected boolean isStream;
    protected boolean prettyPrint = false;

    public boolean isStream() {
	return isStream;
    }

    public boolean prettyprint() {
        return prettyPrint;
    }

    public void dump(java.io.PrintStream ps) {
	
    }
}
