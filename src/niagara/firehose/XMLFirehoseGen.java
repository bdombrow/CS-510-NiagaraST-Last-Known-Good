package niagara.firehose;

import java.io.IOException;

/* base class for all generators used by the Firehose */
abstract class XMLFirehoseGen {

    protected FirehoseSpec fhSpec;
    public abstract String generateXMLString(boolean useStreamingFormat) throws IOException;
}
