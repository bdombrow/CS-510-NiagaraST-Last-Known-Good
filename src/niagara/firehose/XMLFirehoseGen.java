package niagara.firehose;

import java.io.IOException;
import niagara.utils.EOSException;

/* base class for all generators used by the Firehose */
abstract class XMLFirehoseGen {
    //Number of top-level elements to create
    protected int numTLElts;
    protected boolean useStreamingFormat;
    protected boolean usePrettyPrint;
    protected boolean trace;

    //Get the XML
    public abstract byte[] generateXMLBytes() throws IOException, EOSException;

    //Is the stream generator done?
    public boolean getEOF() { return false; }
}
