package niagara.xmlql_parser.op_tree;

/**
 * StreamSpec - specification of a stream -
 * can be a file or a very simple firehose (hostname and port num)
 */

import java.io.*;
import niagara.utils.*;

public class StreamSpec {
    /* also need a streaming media stream!! */
    public static final int FILE = 1;
    public static final int FIREHOSE = 2;
    
    private int type;

    /* for file */
    private String file_name;

    /* for socket */
    private String host_name;
    private int port_num;

    private boolean streaming;

    /**
     * Initialize the stream spec
     */
    public StreamSpec(String file_name, boolean streaming) {
	type = StreamSpec.FILE;
	this.file_name = file_name;
	host_name = null;
	port_num = -1;
        this.streaming = streaming;
    }

    public StreamSpec(String host_name, int port_num, boolean streaming) {
	type = StreamSpec.FIREHOSE;
	this.host_name = host_name; 
	this.port_num = port_num;
	file_name = null;
        this.streaming = streaming;
    }

    public int getType() {
	return type;
    }

    public String getFileName() {
	return file_name;
    }

    public String getHostName() {
	return host_name;
    }

    public int getPortNum() {
	return port_num;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public void dump(PrintStream os) {
	os.println("Stream Specification: ");
	if(type == StreamSpec.FILE) {
	    os.println("File Stream: fileName: " + file_name);
	} else if (type == StreamSpec.FIREHOSE) {
	    os.println("Firehose Stream: host name: " + host_name +
		       " port number: " + port_num);
	} else {
	    throw new PEException("Invalid Stream type");
	}
    }
}
