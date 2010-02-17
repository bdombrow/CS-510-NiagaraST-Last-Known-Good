package niagara.logical.predicates;

import java.util.ArrayList;

import niagara.physical.predicates.FalseImpl;
import niagara.physical.predicates.PredicateImpl;

/***
 * The constant false predicate
 * */
@SuppressWarnings("unchecked")
public class False extends Predicate {
	// There are many falsehoods, we just deal with one.
	private static final False f = new False();

	// False is a singleton 
	private False() {
	}

	public static False getFalse() {
		return f;
	}

	public PredicateImpl getImplementation() {
		return FalseImpl.getFalseImpl();
	}

	public void getReferencedVariables(ArrayList al) {
	}

	public void beginXML(StringBuffer sb) {
		sb.append("<false/>");
	}

	public void childrenInXML(StringBuffer sb) {
	}

	public void toXML(StringBuffer sb) {
	}

	public boolean equals(Object obj) {
		return this == obj;
	}

	public int hashCode() {
		return System.identityHashCode(this);
	}

	public Predicate negation() {
		return True.getTrue();
	}

	public float selectivity() {
		return 0;
	}
}
