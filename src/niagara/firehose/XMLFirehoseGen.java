package niagara.firehose;

import java.io.IOException;
import niagara.utils.*;

/* base class for all generators used by the Firehose */
abstract class XMLFirehoseGen {
    //Number of top-level elements to create
    protected int numTLElts;
    protected boolean useStreamingFormat;
    protected boolean usePrettyPrint;

    //Get the XML
    public String generateXMLString() throws IOException {
	throw new PEException("KT: Invalid call");
    }

    //Is the stream generator done?
    public boolean getEOF() { return false; }

    public boolean generatesStream() { return false;}
    public boolean generatesChars() { return true;}
    public void generateStream(XMLFirehoseThread writer) throws IOException {
	throw new PEException("KT: Invalid call");
    }
}
