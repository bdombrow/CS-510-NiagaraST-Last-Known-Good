package niagara.xmlql_parser.op_tree;

/**
 * FirehoseSpec - specification of a firehose - includes things
 * like DTD to generate from, rate, etc.
 */

import java.io.*;
import FirehoseClient;

public class FirehoseSpec {

    /* see FirehoseSpec constructor for descriptions of what these
     * variables represent
     */
    private int listenerPortNum;
    private String listenerHostName;
    private long rate;
    private int type;
    private String descriptor;
    private int iters;

    public FirehoseSpec() {}

    /**
     * Initialize the firehose spec
     *
     * @param _listenerPortNum  The port number on which the firehose server
     *                           is listening
     * @param _listenerHostName The host where the firehose server is running
     * @param _rate The rate at which the stream should run - currently
     *              not used.
     * @param _type The type of stream - Valid values are  
     *                 FirehoseClient.FCSTREAMTYPE_* (GEN and FILE) are
     *                 what is currently supported
     * @param _descriptor If stream type is file - descriptor is the name
     *                    of the file to read.  If stream type is gen -
     *                    descriptor is the URI of the DTD to generate from
     */
    public FirehoseSpec(int _listenerPortNum, String _listenerHostName,
			long _rate, int _type, String _descriptor,
			int _iters) {
	listenerPortNum = _listenerPortNum;
	listenerHostName = _listenerHostName;
	rate = _rate;
	type = _type;
	descriptor = _descriptor;
	iters = _iters;
	return;
    }

    public void setListenerPortNum(int _listenerPortNum) {
	listenerPortNum = _listenerPortNum;
    }
    
    public void setListenerHostName(String _listenerHostName) {
	listenerHostName = _listenerHostName;
    }

    public void setRate(long _rate) {
	rate = _rate;
    }
	
    public void setType(int _type) {
	type = _type;
    }

    public void setDescriptor(String _descriptor) {
	descriptor = _descriptor;
    }

    public void setIters(int _iters) {
	iters = _iters;
    }
    
    public int getListenerPortNum() {
	return listenerPortNum;
    }

    public String getListenerHostName() {
	return listenerHostName;
    }

    public long getRate() {
	return rate;
    }

    public int getType() {
	return type;
    }

    public String getDescriptor() {
	return descriptor;
    }

    public int getIters() {
	return iters;
    }

    public void dump(PrintStream os) {
	os.println("Firehose Specification: ");
	os.println("  Listener Port " + String.valueOf(listenerPortNum) +
		   "  Listener Host " + listenerHostName);
	String typeString = "unknown type";
	if(type == FirehoseClient.FCSTREAMTYPE_FILE) {
	    typeString = "file";
	} else if (type == FirehoseClient.FCSTREAMTYPE_GEN) {
	    typeString = "gen";
	}

	os.println("  Rate " + String.valueOf(rate) +
		   ", Type " + typeString +
		   ", Descriptor " + descriptor +
		   ", Iterations " + String.valueOf(iters));
    }
}
