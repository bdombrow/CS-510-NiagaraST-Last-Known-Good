package niagara.firehose;

/**
 * FirehoseSpec - specification of a firehose - includes things
 * like DTD to generate from, rate, etc.
 */

import java.io.*;
import niagara.xmlql_parser.op_tree.StreamSpec;

// oh boy, this extension is ugly, but necessary, just don't
// have the energy to do this properly right now
public class FirehoseSpec extends StreamSpec { 

    /* see FirehoseSpec constructor for descriptions of what these
     * variables represent
     */
    private String listenerHostName;
    private int listenerPortNum;
    private int id; // unique number for this firehose stream
    private int dataType;
    private String descriptor;
    private int numGenCalls; // number of times firehose will call generator
    private int numTLElts;     // num top-level elts in XMLB and AUCTION docs
    private long rate;

    /**
     * Initialize the firehose spec
     *
     * @param listenerPortNum  The port number on which the firehose server
     *                           is listening
     * @param listenerHostName The host where the firehose server is running
     * @param rate The rate at which the stream should run - currently
     *              not used.
     * @param type The type of stream - Valid values are  
     *                 FirehoseClient.FCSTREAMTYPE_* (GEN and FILE) are
     *                 what is currently supported
     * @param descriptor If stream type is file - descriptor is the name
     *                    of the file to read.  If stream type is gen -
     *                    descriptor is the URI of the DTD to generate from
     */
    public FirehoseSpec(int listenerPortNum, String listenerHostName,
			int dataType, String descriptor, int numGenCalls,
			int numTLElts, int rate, boolean streaming) {
	this.listenerPortNum = listenerPortNum;
	this.listenerHostName = listenerHostName;
	this.dataType = dataType;
	this.descriptor = descriptor;
	this.numGenCalls = numGenCalls;
	this.numTLElts = numTLElts;
	this.rate = rate;
	this.streaming = streaming;
	return;
    }

    public FirehoseSpec() {

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

    public int getDataType() {
	return dataType;
    }

    public String getDescriptor() {
	return descriptor;
    }

    public int getNumGenCalls() {
	return numGenCalls;
    }

    public int getNumTLElts() {
	return numTLElts;
    }

    public int getId() {
	return id;
    }

    public void dump(PrintStream os) {
	os.println("Firehose Specification: (" + id + ") ");
	os.println("  Listener Port " + String.valueOf(listenerPortNum) +
		   "  Listener Host " + listenerHostName);
	os.println(" DataType " + FirehoseConstants.typeNames[dataType] +
		   ", Descriptor " + descriptor +
		   ", NumGenCalls " + String.valueOf(numGenCalls) +
		   ", NumTopLevelElts " + String.valueOf(numTLElts) +
		   ", Rate " + String.valueOf(rate) +
		   ", Streaming " + String.valueOf(streaming));
    }

    // appends all the firehose spec info into the given string buffer
    public void marshall(StringBuffer s) {
	s.append(id);
	s.append(" ");
	s.append(dataType);
	s.append(" ");
	if(descriptor.length() == 0) {
	    s.append("NULL");
	} else
	    s.append(descriptor);
	s.append(" ");
	s.append(numGenCalls);
	s.append(" ");
	s.append(numTLElts);
	s.append(" ");
	s.append(rate);
	s.append(" ");
	s.append(streaming);
	s.append(" ");
    }

    public void unmarshall(StreamTokenizer input_stream) 
	throws java.io.IOException, CorruptMsgException {
	// unmarshalling is used on firehose side, port and host are
	// not needed
	listenerHostName = null;
	listenerPortNum = -1;
	
	int ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_NUMBER) {
	    throw new CorruptMsgException("Invalid stream id");
	}
	id = (int)input_stream.nval;
	
	ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_NUMBER) {
	    throw new CorruptMsgException("invalid stream data type");
	}
	dataType = (int)input_stream.nval;
	
	ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_WORD) {
	    throw new CorruptMsgException("invalid stream descriptor");
	}
	if(input_stream.sval.equals("NULL"))
	    descriptor = "";
	else
	    descriptor = input_stream.sval;
	
	ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_NUMBER) {
	    throw new CorruptMsgException("invalid num gen calls");
	}
	numGenCalls = (int)input_stream.nval;
	
	ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_NUMBER) {
	    throw new CorruptMsgException("invalid num TL elts");
	}
	numTLElts = (int)input_stream.nval;
	
	ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_NUMBER) {
	    throw new CorruptMsgException("invalid rate");
	}
	rate = (int)input_stream.nval;
	
	ttype = input_stream.nextToken();
	if(ttype != StreamTokenizer.TT_WORD) {
	    throw new CorruptMsgException("invalid streaming value");
	}
	streaming = (Boolean.valueOf(input_stream.sval)).booleanValue();     
    }
}
