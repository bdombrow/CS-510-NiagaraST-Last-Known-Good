import java.io.*;
import javax.xml.parsers.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** Tests speed of SAX parsing an XML file and then reconstructing it */
public class TestSAXOutput extends DefaultHandler {

	static PrintWriter pw;
	static String outFileName;

	public static void main(String argv[]) {
		if (argv.length != 3) {
			System.err
					.println("Usage: java -server TestSAXOutput <xml file> <times to parse> <output file>");
			System.exit(-1);
		}

		String xmlFile = argv[0];
		int times = (new Integer(argv[1])).intValue();
		outFileName = argv[2];

		try {

			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser parser = factory.newSAXParser();
			TestSAXOutput tsp = new TestSAXOutput();
			for (int i = 0; i < times; i++) {
				parser.parse(xmlFile, tsp);
			}
		} catch (FactoryConfigurationError e) {
			System.err.print("factory configuration error:\n" + e);
		} catch (ParserConfigurationException e) {
			System.err.print("parser configuration error:\n" + e);
		} catch (SAXException e) {
			System.err.print("sax configuration error:\n" + e);
		} catch (IOException e) {
			System.err.print("io error:\n" + e);
		}
	}

	public void characters(char[] ch, int start, int length)
			throws SAXException {
		pw.write(ch, start, length);
	}

	public void endDocument() throws SAXException {
		pw.close();
	}

	public void endElement(String namespaceURI, String localName, String qName)
			throws SAXException {
		pw.write("</");
		pw.write(qName);
		pw.write(">");
	}

	public void startElement(String namespaceURI, String localName,
			String qName, Attributes atts) throws SAXException {
		pw.write("<");
		pw.write(qName);

		int length = atts.getLength();
		for (int i = 0; i < length; i++) {
			pw.write(" ");
			pw.write(atts.getQName(i));
			pw.write("='");
			pw.write(atts.getValue(i));
			pw.write("'");
		}

		pw.write(">");
	}

	public void startDocument() throws SAXException {
		try {
			pw = new PrintWriter(
					new BufferedWriter(new FileWriter(outFileName)));
		} catch (IOException e) {
			System.err.print("io error:\n" + e);
		}
	}
}
