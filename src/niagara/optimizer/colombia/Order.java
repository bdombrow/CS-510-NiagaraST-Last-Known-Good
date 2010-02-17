package niagara.optimizer.colombia;

// One-layered order model
public class Order {

	public static class Kind {
		private String name;

		private Kind(String name) {
			this.name = name;
		}

		public String toString() {
			return name;
		}

		public static Kind ANY = new Kind("Any"); // any means any property
		public static Kind HEAP = new Kind("Heap");
		public static Kind SORTED = new Kind("Sorted");
		public static Kind GROUPED = new Kind("Grouped");
		public static Kind HASHED = new Kind("Hashed");
		// assume unique hash function for entire system
	}

	// convenience method
	public static Order newAny() {
		return new Order(Order.Kind.ANY);
	}

	public boolean isAny() {
		return this.kind == Kind.ANY;
	}

	public boolean isSorted() {
		return this.kind == Kind.SORTED;
	}

	private Kind kind;
	Strings attrNames; // the attribute which the order is based on

	// SORT_KIND AD; // If Kind is Sorted, then AD is asending (default)
	// or descending. SORT_AD is a typedef in defs.h

	// Access Functions
	Kind getKind() {
		return kind;
	}

	// SORT_KIND GetAD() { return AD;}

	public Order(Strings attrNames) {
		this.attrNames = attrNames;
		kind = Kind.SORTED;
		// Attributes with the same names will be the same
		// (via equality predicate); Therefore remove duplicates.
		attrNames.distinct();
	}

	private Order(Kind kind, Strings attrNames) {
		this.attrNames = attrNames; // XXX vpapad maybe copy()?
		this.kind = kind;

		// Attributes with the same names will be the same
		// (via equality predicate); Therefore remove duplicates.
		attrNames.distinct();
	}

	Order(Kind kind) {
		this(kind, new Strings());
	}

	public Order(Order other) {
		kind = other.getKind();
		attrNames = other.getAttrNames().copy();
	}

	boolean equals(Order other) {
		if (other.getKind() == Kind.ANY && kind == Kind.ANY)
			return true;

		if (kind != other.getKind())
			return false;

		return attrNames.equals(other.getAttrNames());
	}

	// Order & operator = (Order & other) // = operator
	// {
	// Kind = other.GetKind();
	// //AD = other.GetAD();
	// AttrNames = other.getAttrNames().copy();
	// return * this;
	// }

	// For frequent cases -- one order attribute
	String getAttrName() {
		return attrNames.get(0);
	}

	Strings getAttrNames() {
		return attrNames;
	}

	public Order copy() {
		return new Order(this);
	}

	// Use for sort order test for MergeJoin
	// Where attrs is the left "attrnames" or right "attrnames" of MergeJoin
	// check if the first attribute of any key is among "attrnames"
	boolean satisfyOrder(Strings attrNames) {
		if (kind == Kind.ANY)
			return false;

		if (attrNames.size() == 0)
			return true;

		if (this.attrNames.size() == 0)
			return false;

		// Whether the first attribute names match
		return (getAttrName() == attrNames.get(0));
	}

	public String toString() {
		return kind.toString() + " " + attrNames.toString();
	}
}
