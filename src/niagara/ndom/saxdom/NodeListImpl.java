package niagara.ndom.saxdom;

import java.util.ArrayList;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/***
 * A read-only implementation of the DOM Level 2 interface, using an array of
 * SAX events as the underlying data store.
 * 
 */

@SuppressWarnings("unchecked")
public class NodeListImpl implements NodeList {

	private ArrayList items;

	public NodeListImpl(ArrayList items) {
		this.items = items;
	}

	public Node item(int index) {
		if (index < 0 || index >= items.size())
			return null;
		else
			return (Node) items.get(index);
	}

	public int getLength() {
		return items.size();
	}

}
