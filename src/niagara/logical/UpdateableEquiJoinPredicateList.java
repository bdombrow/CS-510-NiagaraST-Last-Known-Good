/* $Id: UpdateableEquiJoinPredicateList.java,v 1.1 2003/09/16 04:53:35 vpapad Exp $ */
package niagara.logical;

import java.util.*;

import niagara.optimizer.colombia.Attribute;

/** The mutable version of <code>EquiJoinPredicateList</code> */
public class UpdateableEquiJoinPredicateList extends EquiJoinPredicateList {
    /** Constructor with no equijoin predicates */
    public UpdateableEquiJoinPredicateList() {
        super();
    }

    public UpdateableEquiJoinPredicateList(ArrayList left, ArrayList right) {
        super(left, right);
    }

    public void add(Attribute leftAttr, Attribute rightAttr) {
        left.add(leftAttr);
        right.add(rightAttr);
    }

    /** Remove the predicate at position pos*/
    public void remove(int pos) {
        left.remove(pos);
        right.remove(pos);
    }

    public void addAll(EquiJoinPredicateList other) {
        left.addAll(other.left);
        right.addAll(other.right);
    }

    /** Remove unnecessary predicates, using an equivalence classes
     * hashmap (and remove the corresponding entries from the hashmap) */
    public void removeDuplicates(HashMap eqClasses) {
        int predsToCheck = left.size();
        int i = 0;
        while (i < predsToCheck) {
            Attribute la = (Attribute) left.get(i);
            Attribute ra = (Attribute) right.get(i);

            HashSet eqClass1 = (HashSet) eqClasses.get(la);
            HashSet eqClass2 = (HashSet) eqClasses.get(ra);

            boolean asl = eqClass1.remove(ra);
            boolean asr = eqClass2.remove(la);
            // If this predicate is necessary for the equivalence
            // classes of either the left or the right attribute,
            // let it be
            if (asl || asr)
                i++;
            else {
                // the predicate is subsumed by a predicate we checked before
                remove(i);
                predsToCheck--;
            }
        }
    }

    public final boolean equals(Object other) {
        // XXX vpapad: made equals final, dealing with additional subclasses will get
        // really crazy really soon 
        if (other == null
            || !(other instanceof EquiJoinPredicateList))
            return false;
        EquiJoinPredicateList o =
            (EquiJoinPredicateList) other;
        if (size() != o.size())
            return false;
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).equals(o.left.get(i)))
                return false;
            if (!right.get(i).equals(o.right.get(i)))
                return false;
        }
        return true;
    }
}
