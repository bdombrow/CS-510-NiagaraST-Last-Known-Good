/* $Id: BinaryPredicate.java,v 1.1 2002/10/06 23:40:12 vpapad Exp $ */
package niagara.logical;

public abstract class BinaryPredicate extends Predicate {
    public abstract Predicate getLeft();
    public abstract Predicate getRight();
}
