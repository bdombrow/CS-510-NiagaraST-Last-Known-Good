/* $Id: BinaryPredicate.java,v 1.1 2003/12/24 02:03:51 vpapad Exp $ */
package niagara.logical.predicates;


public abstract class BinaryPredicate extends Predicate {
    public abstract Predicate getLeft();
    public abstract Predicate getRight();
}
