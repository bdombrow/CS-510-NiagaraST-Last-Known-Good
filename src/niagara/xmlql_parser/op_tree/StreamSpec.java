package niagara.xmlql_parser.op_tree;

/**
 * StreamSpec - base class for stream specification -
 * extending classes: FileScanSpec, FirehoseScanSpec
 */

public class StreamSpec {
    protected boolean isStream;

    public boolean isStream() {
	return isStream;
    }

    public void dump(java.io.PrintStream ps) {
	
    }
}
