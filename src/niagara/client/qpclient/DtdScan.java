package niagara.client.qpclient;

  import diva.graph.*;
  import diva.graph.model.*;

import java.io.*;

import org.w3c.dom.*;
import niagara.utils.XMLUtils;

public class DtdScan extends Operator {
    String urls = "";
    public DtdScan() {
	super();
	type = "dtdscan";
    }

    protected void saveChildren(PrintWriter pw, GraphView gv) {
	pw.println(urls);
    }


    /**
     * DtdScan cannot have any inputs - replace saveInput with a NOP
     *
     * @param pw a <code>PrintWriter</code> value
     */
    protected void saveInput(PrintWriter pw) {
    }

    public void handleChildren(Element e, IDREFResolver idr, GraphModel gm) {
	NodeList nl = e.getChildNodes();
	for (int i=0; i < nl.getLength(); i++) {
	    org.w3c.dom.Node n = nl.item(i);
	    if (!(n instanceof Element))
		continue;
	    urls = urls + XMLUtils.flatten(n, false);
	}
    }
}
