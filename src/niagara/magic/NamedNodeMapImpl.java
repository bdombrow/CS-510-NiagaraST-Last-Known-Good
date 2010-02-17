package niagara.magic;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class NamedNodeMapImpl implements NamedNodeMap {

	Node[] nodeArray;
	int currIdx;

	public NamedNodeMapImpl() {
		nodeArray = new Node[5];
		currIdx = 0;
	}

	public int getLength() {
		return currIdx;
	}

	public Node getNamedItem(String name) {
		for (int i = 0; i < currIdx; i++) {
			if (nodeArray[i].getNodeName().equals(name))
				return nodeArray[i];
		}
		return null;
	}

	public Node getNamedItemNS(String namespace, String locaName) {
		assert false : "Namespaces not supported in magic code";
		return null;
	}

	public Node item(int index) {
		return nodeArray[index];
	}

	public Node removeNamedItem(String name) {
		assert false : "Updates not supported in magic code";
		return null;
	}

	public Node removeNamedItemNS(String namespace, String localName) {
		assert false : "Namespaces not supported in magic code";
		return null;
	}

	public Node setNamedItem(Node arg) {
		// see if one with this name exists
		for (int i = 0; i < currIdx; i++) {
			if (nodeArray[i].getNodeName().equals(arg.getNodeName())) {
				Node prevNode = nodeArray[i];
				nodeArray[i] = arg;
				return prevNode;
			}
		}

		// if not just add it to the end
		checkSpace();
		nodeArray[currIdx] = arg;
		currIdx++;
		return null;
	}

	public Node setNamedItemNS(Node arg) {
		assert false : "Namespaces not supported in magic code";
		return null;
	}

	private void checkSpace() {
		if (currIdx < nodeArray.length)
			return;
		else {
			assert currIdx == nodeArray.length;
			Node[] newArray = new Node[currIdx * 2];
			for (int i = 0; i < currIdx; i++)
				newArray[i] = nodeArray[i];
			nodeArray = newArray;
		}

	}
}
