package niagara.client.qpclient;

import diva.graph.model.*;
import org.w3c.dom.*;

import java.awt.*;
import java.awt.geom.*;

import java.io.*;
import java.util.*;
import diva.graph.*;

public class Plan extends QPNode {

    public Plan() {
	super();
	in = new ArrayList();
	out = new ArrayList();
	children = new ArrayList();
    }

    String top_idref;
    public void parse(Element e, IDREFResolver idr, GraphModel gm) {
	super.parse(e, idr, gm);
	top_idref = e.getAttribute("top");
	in.add(top_idref);
	idr.addClient(top_idref, this);
	type = "plan";
	
	NodeList nl = e.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            if (!(nl.item(i) instanceof Element))
                continue;

            QPNode qpn = QPNodeFactory.parse((Element) nl.item(i), idr, gm);
            if (qpn != null) {
		children.add(qpn);
	    }
        }
	gm.addNode(this);
    }

    public void save(PrintWriter pw, GraphView gv) {
	pw.println("<?xml version=\"1.0\"?>");
	pw.println("<!DOCTYPE plan SYSTEM \"queryplan.dtd\">");

	pw.print("<" + getType() + " top=\"" + top_idref + "\" ");
	saveCoordinates(pw, gv);
	pw.println(">");
	for (int i=0; i < children.size(); i++) 
	    ((QPNode) children.get(i)).save(pw, gv);
	pw.println("</" + getType() + ">");
    }
    
    public void becomesHead(Edge e) {
	
    }

    public void becomesTail(Edge e) {
	
    }

    public void paint(QPFigure qpf, Graphics2D g) {
	super.paint(qpf, g);
	Rectangle2D b = qpf.getBounds();
	
	String title = getType();
	int titleWidth = g.getFontMetrics().stringWidth(title);
	float midx = (float) b.getX() + (float) (b.getWidth() /  2f) - titleWidth/2;
	float topy = (float) b.getY() + 10f;
	g.drawString(title, midx, topy);
    }
}
