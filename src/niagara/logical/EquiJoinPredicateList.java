/* $Id: EquiJoinPredicateList.java,v 1.3 2003/09/13 03:52:11 vpapad Exp $ */
package niagara.logical;

import java.util.*;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.xmlql_parser.syntax_tree.opType;

/** A conjunction of equality predicates */
public class EquiJoinPredicateList {

    ArrayList left;
    ArrayList right;

    /** Constructor with no equijoin predicates */
    public EquiJoinPredicateList() {
        this.left = new ArrayList();
        this.right = new ArrayList();
    }

    public EquiJoinPredicateList(ArrayList left, ArrayList right) {
        assert left.size() == right.size();
        this.left = new ArrayList(left.size());
        this.left.addAll(left);
        this.right = new ArrayList(right.size());
        this.right.addAll(right);
    }

    public Predicate toPredicate() {
        Predicate p = True.getTrue();
        for (int i = left.size() - 1; i >= 0; i--) {
            Variable la = (Variable) left.get(i);
            Variable ra = (Variable) right.get(i);
            p = And.conjunction(Comparison.newComparison(opType.EQ, la, ra), p);
        }
        return p;
    }

    public void getReferencedVariables(ArrayList al) {
        al.addAll(left);
        al.addAll(right);
    }
    
    public EquiJoinPredicateList copy() {
        return new EquiJoinPredicateList(left, right);
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

    public int size() {
        return left.size();
    }
    
    public boolean isEmpty() {
        return size() == 0;
    }

    /** Attributes on the left hand side of the equality predicates */
    public Attrs getLeft() {
        return new Attrs(left);
    }

    /** Attributes on the left hand side of the equality predicates */
    public Attrs getRight() {
        return new Attrs(right);
    }

    /** Returns a reversed (deep) copy of this EquiJoinPredicateList */
    public EquiJoinPredicateList reversed() {
        return new EquiJoinPredicateList(
            (ArrayList) right.clone(),
            (ArrayList) left.clone());
    }
    
    public HashMap getEquivalenceClasses() {
        // Compute the equivalence classes for A, B, and C attributes
        // XXX vpapad: in Columbia something like this was done in
        // the findLogProp method of EQJOIN. A predicate like A.x = B.y 
        // added A.x to the list of path expressions and attribute 
        // properties of B, and vice versa. This is hard to translate
        // into Colombia (or Niagara!) terms.

        // Maps each attribute to its equivalence class
        HashMap attr2class = new HashMap();

        Attrs leftAttrs = getLeft();
        Attrs rightAttrs = getRight();

        for (int i = 0; i < leftAttrs.size(); i++) {
            Attribute la = leftAttrs.get(i);
            Attribute ra = rightAttrs.get(i);
            HashSet leftEQ = (HashSet) attr2class.get(la);
            HashSet rightEQ = (HashSet) attr2class.get(ra);

            if (leftEQ == null && rightEQ == null) {
                HashSet eqClass = new HashSet();
                eqClass.add(la);
                eqClass.add(ra);
                attr2class.put(la, eqClass);
                attr2class.put(ra, eqClass);
                continue;
            }
            if (leftEQ == null && rightEQ != null) {
                rightEQ.add(la);
                attr2class.put(la, rightEQ);
            } else if (rightEQ == null && leftEQ != null) {
                leftEQ.add(ra);
                attr2class.put(ra, leftEQ);
            } else {
                // We have equivalence classes for both, join them
                HashSet large, small;
                if (leftEQ.size() >= rightEQ.size()) {
                    large = leftEQ;
                    small = rightEQ;
                } else {
                    small = leftEQ;
                    large = rightEQ;
                }
                large.addAll(small);
                Iterator it = small.iterator();
                while (it.hasNext()) {
                    attr2class.put(it.next(), large);
                }
            }
        }
        return attr2class;
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
            if ( asl || asr)
                i++; 
            else {
                // the predicate is subsumed by a predicate we checked before
                remove(i);
                predsToCheck--;
            }
        }
    }    
    
    public void toXML(StringBuffer sb) {
        if (left.size() == 0) return;
        sb.append(" left='").append(((Attribute) left.get(0)).getName());
        for (int i = 1; i < left.size(); i++)
            sb.append(",").append(((Attribute) left.get(i)).getName());
        sb.append("' right='").append(((Attribute) right.get(0)).getName());
        for (int i = 1; i < right.size(); i++)
            sb.append(",").append(((Attribute) right.get(i)).getName());
        sb.append("' ");
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object other) {
        // XXX vpapad: rearranging predicates will make the equals
        // method return false, but handling such permutations is hard
        if (other == null || !(other instanceof EquiJoinPredicateList))
            return false;
        if (other.getClass() != EquiJoinPredicateList.class)
            return other.equals(this);
        EquiJoinPredicateList o = (EquiJoinPredicateList) other;
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

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        int hashCode = 0;
        // XXX vpapad: this will return different hashcodes for permutations
        // of the same equijoin predicate list
        for (int i = 0; i < left.size(); i++) {
            hashCode ^= (left.get(i).hashCode() + i);
            hashCode ^= (right.get(i).hashCode() + i);            
        }
        return hashCode;
    }
}
