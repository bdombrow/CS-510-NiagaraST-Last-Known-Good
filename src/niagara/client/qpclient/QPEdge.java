package niagara.client.qpclient;

import java.io.Serializable;

import diva.graph.model.Edge;
import diva.graph.model.Node;
import diva.util.BasicPropertyContainer;

/**
 * A basic implementation of the Edge interface.
 * 
 * @author Michael Shilman (michaels@eecs.berkeley.edu)
 * @version $Revision: 1.2 $
 * @rating Yellow
 */
@SuppressWarnings("serial")
public class QPEdge extends BasicPropertyContainer implements Edge,
		Serializable {
	/**
	 * The head of the edge.
	 */
	private QPNode _head = null;

	/**
	 * Edge user data.
	 */
	private Object _semanticObject = null;

	/**
	 * The tail of the edge.
	 */
	private QPNode _tail = null;

	/**
	 * Whether or not this edge is directed.
	 */
	private boolean _directed = true;

	/**
	 * The weight on this edge.
	 */
	private double _weight = 1.0;

	/**
	 * Create a new edge with no tail or head.
	 */
	public QPEdge() {
		this(null);
	}

	/**
	 * Create a new edge with the specified user object but no tail or head.
	 */
	public QPEdge(Object userObject) {
		this(userObject, null, null);
	}

	/**
	 * Create a new edge with the specified tail/head.
	 */
	public QPEdge(QPNode tail, QPNode head) {
		this(null, tail, head);
	}

	/**
	 * Create a new edge with the specified user object and tail/head
	 */
	public QPEdge(Object userObject, QPNode tail, QPNode head) {
		setSemanticObject(userObject);
		attach(tail, head);
	}

	public void attach(QPNode tail, QPNode head) {
		setTail(tail);
		setHead(head);
	}

	public void detach() {
		setTail(null);
		setHead(null);
	}

	public Node getHead() {
		return _head;
	}

	public Node getTail() {
		return _tail;
	}

	public double getWeight() {
		return _weight;
	}

	public Object getSemanticObject() {
		return _semanticObject;
	}

	public boolean isDirected() {
		return _directed;
	}

	public void setDirected(boolean val) {
		_directed = val;
	}

	@SuppressWarnings("unchecked")
	public void setHead(QPNode n) {
		QPNode prevHead = (QPNode) getHead();
		if (prevHead != null) {
			prevHead.getInEdges().remove(this);
		}

		_head = n;
		if (_head != null) {
			_head.getInEdges().add(this);
			// _head.getInEdges().add(this);
		}
	}

	@SuppressWarnings("unchecked")
	public void setTail(QPNode n) {
		QPNode prevTail = (QPNode) getTail();
		if (prevTail != null) {
			prevTail.getOutEdges().remove(this);
		}

		_tail = n;
		if (_tail != null) {
			_tail.getOutEdges().add(this);
		}
	}

	public void setWeight(double weight) {
		_weight = weight;
	}

	public void setSemanticObject(Object o) {
		_semanticObject = o;
	}

	public String toString() {
		Object o = getSemanticObject();
		if (o != null) {
			return "QPEdge[" + o.toString() + "]";
		} else {
			return super.toString();
		}
	}
}
