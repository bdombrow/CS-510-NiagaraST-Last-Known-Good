package niagara.xmlql_parser.op_tree;

/**
 * StreamSpec - base class for stream specification -
 * extending classes: FileScanSpec, FirehoseScanSpec
 */

public class StreamSpec {
       protected boolean streaming;
       public boolean isStreaming() {
            return streaming;
       }
}
