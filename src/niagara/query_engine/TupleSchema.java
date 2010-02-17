package niagara.query_engine;

import java.util.ArrayList;
import java.util.HashMap;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;

/** A tuple schema maps attribute/variable names to tuple positions. */
@SuppressWarnings("unchecked")
public class TupleSchema {
	/** Length of the tuple */
	private int length;

	/** Maps variable name to tuple position */
	private HashMap name2pos;

	/** Maps variable name to variable */
	private HashMap name2var;

	/** Maps tuple position to variable name */
	private HashMap pos2name;

	public TupleSchema() {
		length = 0;
		name2pos = new HashMap();
		name2var = new HashMap();
		pos2name = new HashMap();
	}

	/** Deep copy of this tuple schema */
	public TupleSchema copy() {
		TupleSchema ts = new TupleSchema();
		for (int i = 0; i < length; i++) {
			Attribute attr = getVariable(i);
			ts.addMapping(attr);
		}
		return ts;
	}

	public void addMappings(Attrs attrs) {
		for (int i = 0; i < attrs.size(); i++) {
			addMapping(attrs.get(i));
		}
	}

	/**
	 * Create a new tuple schema that contains all the attributes in attrs, with
	 * the attributes shared between attrs and this schema appearing first
	 **/
	public TupleSchema project(Attrs attrs) {
		// Current attributes
		Attrs currentAttrs = getAttrs();

		// Attributes that appear in both attrs and the current schema
		Attrs keptAttrs = currentAttrs.project(attrs);

		// Attributes that appear in attrs but not in this schema
		Attrs addedAttrs = attrs.minus(currentAttrs);

		TupleSchema ts = new TupleSchema();

		ts.addMappings(keptAttrs);
		ts.addMappings(addedAttrs);

		return ts;
	}

	/**
	 * Returns an array where the i-th element contains the position of the i-th
	 * variable of ts in this schema
	 */
	public int[] mapPositions(TupleSchema ts) {
		// How many attributes are shared with ts
		int shared = ts.getAttrs().project(getAttrs()).size();
		int[] map = new int[shared];

		for (int i = 0; i < shared; i++)
			map[i] = getPosition(ts.getVariableName(i));

		return map;
	}

	/** Map a name to a new field */
	public void addMapping(Attribute var) {
		String name = var.getName();
		assert name2pos.get(name) == null : "Duplicate variable name";
		Integer pos = new Integer(length);
		length++;
		name2pos.put(name, pos);
		name2var.put(name, var);
		pos2name.put(pos, name);
	}

	public int getLength() {
		return length;
	}

	public boolean contains(String name) {
		return (name2pos.get(name) != null);
	}

	public int getPosition(String name) {
		return ((Integer) name2pos.get(name)).intValue();
	}

	public Attribute getVariable(int position) {
		return (Attribute) name2var.get(getVariableName(position));
	}

	public String getVariableName(int position) {
		return (String) pos2name.get(new Integer(position));
	}

	public ArrayList getVariables() {
		ArrayList al = new ArrayList(length);
		for (int i = 0; i < length; i++) {
			al.add(getVariable(i));
		}
		return al;
	}

	public Attrs getAttrs() {
		return new Attrs(getVariables());
	}
}
