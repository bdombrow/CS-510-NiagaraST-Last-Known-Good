package niagara.xmlql_parser.op_tree;

/**
 * StreamSpec - specification of a stream -
 * can be a file or a very simple firehose (hostname and port num)
 */

import java.io.*;
import niagara.utils.*;

public class FileScanSpec extends StreamSpec{
    private String file_name;

    /**
     * Initialize the stream spec
     */
    public FileScanSpec(String file_name) {
	this.file_name = file_name;
    }

    public String getFileName() {
	return file_name;
    }

    public void dump(PrintStream os) {
	os.println("Stream Specification: ");
	os.println("File Stream: fileName: " + file_name);
    }
}
