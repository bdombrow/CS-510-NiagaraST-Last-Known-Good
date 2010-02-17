package niagara.client.qpclient;

import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import diva.graph.GraphView;
import diva.graph.model.Edge;
import diva.graph.model.GraphModel;

@SuppressWarnings("unchecked")
public class Operator extends QPNode {

	public Operator() {
		in = new ArrayList();
		out = new ArrayList();
		children = new ArrayList();
	}

	public void parse(Element e, IDREFResolver idr, GraphModel gm) {
		super.parse(e, idr, gm);
		String id = e.getAttribute("id");
		idr.addIDREF(id, this);

		String input = e.getAttribute("input");
		Vector inputs = new Vector();
		StringTokenizer st = new StringTokenizer(input);
		while (st.hasMoreTokens())
			inputs.addElement(st.nextToken());

		for (int i = 0; i < inputs.size(); i++) {
			in.add(inputs.elementAt(i));
			idr.addClient((String) inputs.elementAt(i),
					(IDREFResolverClient) this);
		}

		handleAttributes(e, idr);
		handleChildren(e, idr, gm);
		gm.addNode(this);
		domElement = e;
	}

	public void save(PrintWriter pw, GraphView gv) {
		pw.print("<" + getType() + " ");
		Iterator attrs = propertyKeys();
		while (attrs.hasNext()) {
			String key = (String) attrs.next();
			// Ignore properties starting with _
			if (key.indexOf("_") == 0)
				continue;
			pw.print(key + "=\"" + getProperty(key) + "\" ");
		}

		// Save input id's
		saveInput(pw);

		saveCoordinates(pw, gv);
		pw.println(">");

		// Save children
		saveChildren(pw, gv);
		pw.println("</" + getType() + ">");
	}

	protected void saveInput(PrintWriter pw) {
		pw.print("input=\"");
		for (int i = 0; i < in.size(); i++) {
			QPNode inp = (QPNode) ((QPEdge) in.get(i)).getTail();
			pw.print(inp.getProperty("id"));
			if (i != in.size() - 1)
				pw.print(" ");
		}
		pw.print("\" ");
	}

	protected void saveChildren(PrintWriter pw, GraphView gv) {
		for (int i = 0; i < children.size(); i++) {
			QPNode qpn = (QPNode) children.get(i);
			qpn.save(pw, gv);
		}
	}

	/**
	 * <code>handleAttributes</code> can be overriden to specify the way
	 * attributes are handled the default implementation registers all such
	 * attributes as properties.
	 * 
	 * @param e
	 *            a DOM <code>Element</code>
	 * @param idr
	 *            an <code>IDREFResolver</code>
	 */
	public void handleAttributes(Element e, IDREFResolver idr) {
		NamedNodeMap nnm = e.getAttributes();
		for (int i = 0; i < nnm.getLength(); ++i) {
			Attr attr = (Attr) nnm.item(i);
			String name = attr.getName();
			if (name.equals("input") || name.equals("x") || name.equals("y"))
				continue; // Took care of these already

			String value = attr.getValue();
			setProperty(name, value);
		}
	}

	public void handleChildren(Element e, IDREFResolver idr, GraphModel gm) {
		// Placeholder for subclasses to handle children
	}

	public void becomesHead(Edge e) {

	}

	public void becomesTail(Edge e) {

	}

	// Alignment
	static final short CENTER = 0;
	static final short LEFT = 1;
	static final short RIGHT = 2;

	public class LineDescription {
		public String line;
		Font font;
		short where;

		public LineDescription(String line, short where, Font font) {
			this.line = line;
			this.where = where;
			this.font = font;
		}
	}

	LineDescription[] lines;

	public void defaultSetLines() {
		lines = new LineDescription[] { new LineDescription(getType(), CENTER,
				null) };
	}

	public void setLines(LineDescription[] lines) {
		this.lines = lines;
	}

	public Rectangle2D manageSize(LineDescription lines[], QPFigure qpf,
			Graphics2D g) {
		int width = 0, height = 0;
		Rectangle2D.Float bounds = (Rectangle2D.Float) qpf.getBounds();

		Font current_font = g.getFont();
		for (int i = 0; i < lines.length; i++) {
			g.setFont(lines[i].font);
			Rectangle2D.Float b = (Rectangle2D.Float) g.getFontMetrics()
					.getStringBounds(lines[i].line, g);

			width = (int) Math.max(width, b.getWidth());
			height += b.getHeight();
		}

		g.setFont(current_font);
		bounds.width = Math.max(width + 10, 80);
		bounds.height = 30;

		return bounds;
	}

	public void displayLines(LineDescription lines[], QPFigure qpf, Graphics2D g) {
		Rectangle2D.Float fbounds = (Rectangle2D.Float) qpf.getBounds();
		int width = (int) fbounds.width;
		int height = (int) fbounds.height;
		int org_x = (int) fbounds.x;
		int org_y = (int) fbounds.y - 5;

		Font current_font = g.getFont();

		for (int i = 0; i < lines.length; i++) {
			g.setFont(lines[i].font);
			int stringwidth = g.getFontMetrics().stringWidth(lines[i].line);
			int y = org_y + (i + 1) * height / lines.length;
			switch (lines[i].where) {
			case CENTER:
				g.drawString(lines[i].line, org_x + (width - stringwidth) / 2,
						y);
				break;
			case LEFT:
				g.drawString(lines[i].line, org_x, y);
				break;
			case RIGHT:
				g.drawString(lines[i].line, org_x + width - stringwidth, y);
				break;
			}
		}
		g.setFont(current_font);
	}

	public void paint(QPFigure qpf, Graphics2D g) {
		if (lines == null)
			defaultSetLines();
		// calculate proper height/width and set it
		Rectangle2D bounds = manageSize(lines, qpf, g);
		qpf.setBounds(bounds);

		super.paint(qpf, g);

		// Then, display lines
		displayLines(lines, qpf, g);
	}
}
