package niagara.firehose;

import java.io.IOException;

/* base class for all generators used by the Firehose */
abstract class XMLFirehoseGen {
    protected int numTLElts;
    protected boolean useStreamingFormat;
    protected boolean usePrettyPrint;

    public abstract String generateXMLString() throws IOException;
}
