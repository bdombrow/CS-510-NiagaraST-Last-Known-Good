package niagara.logical;

/**
 * StreamSpec - specification of a stream -
 * can be a file or a very simple firehose (hostname and port num)
 */

import java.io.*;

public class FileScanSpec extends StreamSpec{
    private String file_name;

    /**
     * Initialize the stream spec
     */
    public FileScanSpec(String file_name, boolean isStream, int delay) {
	this.file_name = file_name;
	this.isStream = isStream;
	this.delay = delay;
    }

    public String getFileName() {
	return file_name;
    }

    public void dump(PrintStream os) {
	os.println("Stream Specification: ");
	os.println("File Stream: fileName: " + file_name);
	os.println("Delay: " + delay);
    }
}
