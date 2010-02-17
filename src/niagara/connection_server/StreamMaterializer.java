package niagara.connection_server;

import java.util.Hashtable;

import niagara.utils.ControlFlag;
import niagara.utils.ShutdownException;
import niagara.utils.SourceTupleStream;
import niagara.utils.Tuple;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

@SuppressWarnings("unchecked")
public class StreamMaterializer extends Thread {
	int number;
	MQPHandler handler;
	SourceTupleStream inputStream;
	StringBuffer output;
	Hashtable elements;

	public void run() {
		try {
			output.append("<stream>");
			while (true) {
				Tuple ste = inputStream.getTuple(500);
				if (ste == null) {
					if (inputStream.getCtrlFlag() == ControlFlag.EOS) {
						break;
					} else {
						// if we time out or get a control flag other than
						// eos, we continue...
						continue;
					}
				}
				process(ste);
			}
			output.append("</stream>");
			handler.subPlanDone(number, output.toString());
		} catch (java.lang.InterruptedException e) {
			System.err.println("KT thread interruped in StreamMaterializer");
			return; // just quit
		} catch (ShutdownException se) {
			return; // just quit
		}
	}

	public void process(Tuple ste) {
		output.append("<tuple>");
		for (int i = 0; i < ste.size(); i++) {
			Object o = ste.getAttribute(i);
			// If o is a Document, serialize its root element
			if (o instanceof Document) {
				o = ((Document) o).getDocumentElement();
			}
			// Now it must be an Element, are you sure? - couldn't
			// it be an attribute?? KT
			Element e = (Element) o;

			output.append("<elt>");
			serialize(e);
			output.append("</elt>");
		}
		output.append("</tuple>");
	}

	protected void serialize(Object o) {
		// XXX we don't handle attributes
		if (o == null)
			return;
		if (o instanceof Text)
			output.append(((Text) o).getData());

		else if (o instanceof Element) {
			if (elements.containsKey(o)) {
				output.append("<eltref eid='" + elements.get(o) + "'/>");
			} else {
				String eid = nextId();
				output.append("<" + ((Element) o).getTagName() + " eid='" + eid
						+ "'>");
				NodeList nl = ((Element) o).getChildNodes();
				for (int i = 0; i < nl.getLength(); i++) {
					serialize(nl.item(i));
				}
				output.append("</" + ((Element) o).getTagName() + ">");
				elements.put(o, eid);
			}
		} else {
			System.out.println("XXX ignoring attribute of type: "
					+ o.getClass());
		}
	}

	int id = 0;

	String nextId() {
		return "" + ++id;
	}

	public String getOutput() {
		String ret = output.toString();
		output.setLength(0);
		return ret;
	}

	public StreamMaterializer() {
		output = new StringBuffer();
		elements = new Hashtable();
	}

	public StreamMaterializer(int number, MQPHandler handler,
			SourceTupleStream inputStream) {
		this();
		this.number = number;
		this.handler = handler;
		this.inputStream = inputStream;
	}
}
