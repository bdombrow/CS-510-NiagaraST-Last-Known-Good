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
            for (int i = 0; i < times; i++) {
                parser.parse(xmlFile, new TestSAXParser());
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

