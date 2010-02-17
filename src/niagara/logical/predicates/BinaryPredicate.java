package niagara.logical.predicates;

public abstract class BinaryPredicate extends Predicate {
	public abstract Predicate getLeft();

	public abstract Predicate getRight();
}
