package niagara.logical;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.physical.MTException;
import niagara.physical.MergeTree;

import org.w3c.dom.Element;

/**
 * This class is a "logical" operator with physical info for the Accumulate
 * operation. Accumulate is a unary operator which takes in a stream of XML
 * documents or fragments and merges them together to create a new XML document.
 */

public class Accumulate extends UnaryOperator {

	private MergeTree mergeTree;
	private Attribute mergeAttr;
	private String accumFileName;
	private String initialAccumFile;
	/** clear existing accum file or not */
	boolean clear;

	/**
	 * Creates the Accumulate. Traverses the mergeTemplate tree and creates a
	 * MergeTree object.
	 * 
	 * @param mergeTempl
	 *            The file name (or perhaps URI) where the merge template is
	 *            located
	 * 
	 * @param mergeIndex
	 *            The index in the tuple structure of the XML
	 *            documents/fragments to be merged
	 */
	public void setAccumulate(String _mergeTemplate, Attribute _mergeAttr,
			String _accumFileName, String _initialAccumFile, boolean _clear)
			throws MTException {
		mergeAttr = _mergeAttr;
		mergeTree = new MergeTree();
		accumFileName = _accumFileName;
		initialAccumFile = _initialAccumFile;
		clear = _clear;

		/* true indicates that accumulate constraints should be checked */
		mergeTree.create(_mergeTemplate, true);
		return;
	}

	/**
	 * Returns the mergeTree for this operator.
	 */
	public MergeTree getMergeTree() {
		return mergeTree;
	}

	public Attribute getMergeAttr() {
		return mergeAttr;
	}

	public String getAccumFileName() {
		return accumFileName;
	}

	public String getInitialAccumFile() {
		return initialAccumFile;
	}

	public boolean getClear() {
		return clear;
	}

	public void dump() {
		System.out.println("Accumulate Operator: ");
		mergeTree.dump(System.out);
		System.out.println("MergeIndex " + mergeAttr.getName());
		if (accumFileName != null) {
			System.out.println("AccumFileName " + accumFileName);
		}
		System.out.println("Selected Algo "
				+ String.valueOf(selectedAlgorithmIndex));
		System.out.println();
	}

	public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
		return new LogicalProperty(0, new Attrs(), input[0].isLocal());
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Accumulate))
			return false;
		if (obj.getClass() != Accumulate.class)
			return obj.equals(this);
		Accumulate other = (Accumulate) obj;
		return mergeTree.equals(other.mergeTree)
				&& mergeAttr.equals(other.mergeAttr)
				&& accumFileName.equals(other.accumFileName)
				&& initialAccumFile.equals(other.initialAccumFile)
				&& clear == other.clear;
	}

	public Op opCopy() {
		Accumulate op = new Accumulate();
		// Just a shallow copy of all members for now
		op.mergeAttr = mergeAttr;
		op.mergeTree = mergeTree;
		op.initialAccumFile = initialAccumFile;
		op.accumFileName = accumFileName;
		op.clear = clear;
		return op;
	}

	public int hashCode() {
		return mergeTree.hashCode() ^ mergeAttr.hashCode()
				^ accumFileName.hashCode() ^ initialAccumFile.hashCode()
				^ (clear ? 1 : 0);
	}

	public void loadFromXML(Element e, LogicalProperty[] inputProperties,
			Catalog catalog) throws InvalidPlanException {
		try {
			/*
			 * Need to create an AccumulateOp 1) The MergeTemplate 2) The
			 * MergeIndex - index of attribute to work on
			 */
			// String id = e.getAttribute("id");

			/* Either a file name, URI or Merge template string */
			String mergeTemplate = e.getAttribute("mergeTemplate");

			/*
			 * input specifies input operator, index specifies index of
			 * attribute to work on
			 */
			// String inputAttr = e.getAttribute("input");
			String mergeAttr = e.getAttribute("mergeAttr");

			/* name by which the accumulated file should be referred to */
			String accumFileName = e.getAttribute("accumFileName");

			/* file containing the initial input to the accumulate */
			String initialAccumFile = e.getAttribute("initialAccumFile");

			String clear = e.getAttribute("clear");
			boolean cl = (!clear.equals("false"));

			LogicalProperty inputLogProp = inputProperties[0];
			Attribute mergeAttribute = Variable.findVariable(inputLogProp,
					mergeAttr);

			setAccumulate(mergeTemplate, mergeAttribute, accumFileName,
					initialAccumFile, cl);
		} catch (MTException mte) {
			throw new InvalidPlanException("Invalid Merge Template"
					+ mte.getMessage());
		}
	}
}
