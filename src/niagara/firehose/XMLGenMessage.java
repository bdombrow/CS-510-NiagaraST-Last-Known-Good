package niagara.firehose;

import java.lang.*;
import java.io.*;
import java.net.*;

// class to hold a message describing the XML stream to be generated -
// for now the class is simple, will probably need to be extended later

class XMLGenMessage {
    private FirehoseSpec fhSpec;
    private Socket client_socket;
    
    public XMLGenMessage() {
        fhSpec = new FirehoseSpec();
    }
    
    public void parse(Reader _reader) throws CorruptMsgException, 
    ShutdownSystemException {    
	try {
	    // convert a text string into parsed elements
	    StreamTokenizer input_stream = new StreamTokenizer(_reader);
	    input_stream.wordChars('/','/');
	    input_stream.wordChars(':',':');
	    input_stream.wordChars('~','~');
	    input_stream.wordChars('_','_');

	    int ttype;
	    ttype = input_stream.nextToken();

	    if(ttype != StreamTokenizer.TT_NUMBER) {
		throw new CorruptMsgException("Invalid Message type");
	    }

	    // Three possible types of messages: OPEN, CLOSE, SHUTDOWN
	    int msgType = (int)input_stream.nval;
	    if(msgType == FirehoseConstants.OPEN) {
		fhSpec.unmarshall(input_stream);
		// eat up last space??
	    } else if(msgType == FirehoseConstants.SHUTDOWN) {
		throw new ShutdownSystemException();
	    } else {
		throw new CorruptMsgException("invalid message type");
	    }
	} catch (IOException e) {
	    throw new CorruptMsgException(e.getMessage());
	}
	return;
    }
    
    public FirehoseSpec getSpec() {
	return fhSpec;
    }

    public void set_client_socket(Socket client_socket) {
	this.client_socket = client_socket;
    }

    public Socket get_client_socket() {
	return client_socket;
    }
}
