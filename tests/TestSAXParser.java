/** $Id: TestSAXParser.java,v 1.3 2002/12/10 01:10:20 vpapad Exp $ */
import java.io.*; 
import javax.xml.parsers.*;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TestSAXParser extends DefaultHandler {

    public static void main(String argv[]) {
        String xmlFile = argv[0];
        int times = (new Integer(argv[1])).intValue();
        
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser parser = factory.newSAXParser();
	    TestSAXParser tsp = new TestSAXParser();
            for (int i = 0; i < times; i++) {
                parser.parse(xmlFile, tsp);
            }
        } 
        catch (FactoryConfigurationError e) {
            System.err.print("factory configuration error:\n" + e);
        } 
        catch (ParserConfigurationException e) {
            System.err.print("parser configuration error:\n" + e);
        }
        catch (SAXException e) {
            System.err.print("sax configuration error:\n" + e);
        } 
        catch (IOException e) {
            System.err.print("io error:\n" + e);
        }
    }
}

