package niagara.client.qpclient;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.w3c.dom.Element;

import diva.canvas.AbstractFigure;
import diva.canvas.CanvasUtilities;
import diva.canvas.Figure;
import diva.graph.GraphView;
import diva.graph.model.Edge;
import diva.graph.model.Graph;
import diva.graph.model.GraphModel;
import diva.graph.model.Node;

@SuppressWarnings("unchecked")
abstract public class QPNode implements Node, IDREFResolverClient {
	ArrayList in;
	ArrayList out;

	Graph parent;
	QPNode parent_node;

	ArrayList children; // Nodes lexically contained in this one

	protected String type;

	public String getType() {
		return type;
	}

	public QPNode getParentNode() {
		return parent_node;
	}

	public void setParentNode(QPNode parent_node) {
		this.parent_node = parent_node;
	}

	public void IDREFResolved(String idref, Object res) {
		replace_idrefs(in, idref, res);
	}

	private void replace_idrefs(ArrayList al, String idref, Object res) {
		for (int i = 0; i < al.size(); ++i) {
			Object o = al.get(i);

			if (o.getClass() == String.class) {
				// A placeholder for the resolved idref
				String s = (String) o;
				if (s.equals(idref)) {
					al.set(i, new QPEdge((QPNode) res, this));
					// Ugly hack: QPEdge inserts itself at node objects
					// by default - remove it from this one!
					al.remove(al.size() - 1);
				}
			}
		}
	}

	public Graph getParent() {
		return parent;
	}

	public void setParent(Graph parent) {
		this.parent = parent;
	}

	public ArrayList getInEdges() {
		return in;
	}

	public ArrayList getOutEdges() {
		return out;
	}

	public Iterator inEdges() {
		return in.iterator();
	}

	public Iterator outEdges() {
		return out.iterator();
	}

	abstract public void becomesHead(Edge e);

	abstract public void becomesTail(Edge e);

	public void save(PrintWriter pw, GraphView gv) {
		System.out.println("XXX can't be here!");
	}

	public void saveCoordinates(PrintWriter pw, GraphView gv) {
		// Save coordinates
		Rectangle2D bounds = gv.getNodeFigure(this).getBounds();
		pw
				.print("x=\"" + (int) (bounds.getX() + bounds.getWidth() / 2)
						+ "\" ");
		pw
				.print("y=\"" + (int) (bounds.getY() + bounds.getHeight() / 2)
						+ "\"");
	}

	org.w3c.dom.Element domElement;

	public void parse(Element e, IDREFResolver idr, GraphModel gm) {
		domElement = e;
	}

	public Element getDOMElement() {
		return domElement;
	}

	private boolean visited;

	public boolean isVisited() {
		return visited;
	}

	public void setVisited(boolean visited) {
		this.visited = visited;
	}

	private HashMap mapping = new HashMap();

	public int getNumProperties() {
		return mapping.size();
	}

	public Iterator propertyKeys() {
		return mapping.keySet().iterator();
	}

	public Object getProperty(String key) {
		return mapping.get(key);
	}

	public void setProperty(String key, Object value) {
		mapping.put(key, value);
	}

	private Object semanticObject;

	public Object getSemanticObject() {
		return semanticObject;
	}

	public void setSemanticObject(Object semanticObject) {
		this.semanticObject = semanticObject;
	}

	public class QPFigure extends AbstractFigure {
		// The bounds of the figure
		private Rectangle2D.Float _bounds;

		// The corresponding node
		QPNode qpn;

		public void setBounds(Rectangle2D bounds) {
			_bounds = (Rectangle2D.Float) bounds;
			_bounds.x = _bounds.x + 0.5f;
			_bounds.y = _bounds.y + 0.5f;
		}

		/**
		 * Create a new instance of this figure. All we do here is take the
		 * coordinates that we have been given and remember them as a rectangle.
		 * In general, we may want several constructors, and methods to set and
		 * get fields that will control the visual properties of the figure.
		 */

		public QPFigure(QPNode qpn) {
			this.qpn = qpn;
			_bounds = new Rectangle2D.Float(0, 0, 80, 30);
		}

		/**
		 * Get the bounds of this figure. Because, in this example, we have
		 * stroked the outline of the rectangle, we have to create a new
		 * rectangle that is the bounds of the outside of that stroke. In this
		 * method the stroke object is being created each time, but it would
		 * normally be created only once.
		 */
		public Rectangle2D getBounds() {
			Stroke s = new BasicStroke(1.0f);
			return s.createStrokedShape(_bounds).getBounds2D();
		}

		/**
		 * Get the shape of this figure. In this example, it's just the bounding
		 * rectangle that we stored in the constructor. Note that in general,
		 * figures assume that clients will not modify the object returned by
		 * this method.
		 */
		public Shape getShape() {
			return _bounds;
		}

		public void paint(Graphics2D g) {
			qpn.paint(this, g);
		}

		/**
		 * Transform the object. There are various ways of doing this, some more
		 * complicated and some even morer complicated... In this example, we
		 * use a utility function in the class diva.canvas.CanvasUtils to
		 * transform the bounding box. Both before and after we do the
		 * transformation, we have to call the repaint() method so that the
		 * region of the canvas that changed is properly repainted.
		 */
		public void transform(AffineTransform at) {
			repaint();
			_bounds = (Rectangle2D.Float) CanvasUtilities
					.transform(_bounds, at);
			repaint();
		}
	}

	protected Color nodeColor = Color.orange;
	protected Color borderColor = Color.black;

	public void paint(QPFigure qpf, Graphics2D g) {
		// Create a stroke and fill then outline the rectangle
		Stroke s = new BasicStroke(1.0f);
		g.setStroke(s);
		g.setPaint(nodeColor);
		g.fill(qpf.getBounds());
		g.setPaint(borderColor);
		g.draw(qpf.getBounds());
	}

	public Figure render() {
		return new QPFigure(this);
	}

}
