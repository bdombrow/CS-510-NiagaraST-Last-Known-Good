package niagara.firehose;

import java.io.*;

// similar to a generator, but just reads from a file. Yes, ugly, but will be really fast.

class XMLFileReader extends XMLFirehoseGen {

    private String filename;
    private byte[] byteArray;
    static int ARR_LEN = 8192;
    private BufferedInputStream is;
    private XMLFirehoseThread writer;

    public XMLFileReader(String filename, boolean streaming) 
	throws FileNotFoundException {

	this.filename = filename;
	useStreamingFormat = streaming;
	usePrettyPrint = false;

	byteArray = new byte[ARR_LEN];

	// open the file
	is = new BufferedInputStream(new FileInputStream(filename));
    }

    public boolean generatesStream() {
	return true;
    }

    public boolean generatesChars() {
	return false; // generates bytes
    }
  
    public void generateStream(XMLFirehoseThread writer) throws IOException {
	this.writer = writer;
	int bytesRead = 0;
	while(bytesRead != -1) {
	    bytesRead = is.read(byteArray, 0, ARR_LEN);
	    if(bytesRead != -1)
		writer.write_bytes(byteArray, bytesRead);
	} 
    }
}
