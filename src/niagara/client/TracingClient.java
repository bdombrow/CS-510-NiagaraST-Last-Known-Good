package niagara.client;

import java.io.*;
import java.util.*;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

public class TracingClient extends SimpleClient {
    boolean firstTime;
    public TracingClient(String host, int port, String outputFileName) {
	cm = new ConnectionManager(
	    new TracingConnectionReader(this, host, port,
					new DTDCache(), outputFileName));
	firstTime = true;
    }

    public void processQuery(String queryText) {
	if (firstTime)
	    firstTime = false;
	else
	    cm = new ConnectionManager(
		new TracingConnectionReader(this, host, port,
					    new DTDCache(), outputFileName));

        m_start = System.currentTimeMillis();
        final int id =
            cm.executeQuery(
                new SynchronousQPQuery(queryText),
                Integer.MAX_VALUE);
    }

    public void notifyFinalResult(int id) {
	queryDone();
    }

    public static void main(String args[]) {
        parseArguments(args);
        TracingClient tc = new TracingClient(host, port, outputFileName);
        tc.processQueries();
    }
}
