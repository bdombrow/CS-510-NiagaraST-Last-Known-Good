package niagara.firehose;

public class FirehoseConstants {
    public static final int XMLB = 0;
    public static final int SUBFILE = 1;
    public static final int TEMP = 2;
    public static final int AUCTION = 3;
    public static final int DTD = 4;
    public static final int PACKET = 5;
    public static final int AUCTION_STREAM = 6;
    public static final int FILE = 7;

    public static final String typeNames[] = {"xmlb", "subfile", "temp",
					      "auction", "dtd", "packet",
                                              "auction_stream", "file"};
    
    public static final int numDataTypes = 8;

    // types for messages
    public static final int OPEN = 0;
    public static final int SHUTDOWN = 1;

    public static final String messageTypes[] = {"OPEN", "SHUTDOWN"};

    public static final int numMsgTypes = 2;

    public static final String OPEN_STREAM = "<niagara:stream xmlns:niagara = \"http://www.cse.ogi.edu/dot/niagara\">";

    public static final String CLOSE_STREAM = "</niagara:stream>";

    public static int LISTEN_PORT = 5000;
}



