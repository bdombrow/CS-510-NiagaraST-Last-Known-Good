/* $Id: Exchange.java,v 1.1 2003/09/13 03:44:02 vpapad Exp $ */
package niagara.optimizer.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import niagara.optimizer.colombia.*;
import niagara.logical.*;
import niagara.xmlql_parser.op_tree.joinOp;

/** The Exchange rule: (A x B) x (C x D) -> (A x C) x  (B x D) */
public class Exchange extends CustomRule {
    public Exchange(String name) {
        super(
            name,
            4,
            new Expr(
                new joinOp(),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(0)),
                    new Expr(new LeafOp(1))),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(2)),
                    new Expr(new LeafOp(3)))),
            new Expr(
                new joinOp(),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(0)),
                    new Expr(new LeafOp(2))),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(1)),
                    new Expr(new LeafOp(3)))));
    }

    public Expr nextSubstitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        // Get schemas for A, B, C, and D 
        Expr AB = before.getInput(0);
        LeafOp A = (LeafOp) (AB.getInput(0).getOp());
        Attrs aAttrs = A.getGroup().getLogProp().getAttrs();
        LeafOp B = (LeafOp) (AB.getInput(1).getOp());
        Attrs bAttrs = B.getGroup().getLogProp().getAttrs();

        Expr CD = before.getInput(1);
        LeafOp C = (LeafOp) (CD.getInput(0).getOp());
        Attrs cAttrs = A.getGroup().getLogProp().getAttrs();
        LeafOp D = (LeafOp) (CD.getInput(1).getOp());
        Attrs dAttrs = B.getGroup().getLogProp().getAttrs();

        // Old join operators and predicates
        joinOp abcdJoin = (joinOp) before.getOp();
        EquiJoinPredicateList abcdPreds = abcdJoin.getEquiJoinPredicates();
        joinOp abJoin = (joinOp) before.getInput(0).getOp();
        EquiJoinPredicateList abPreds = abJoin.getEquiJoinPredicates();
        joinOp cdJoin = (joinOp) before.getInput(1).getOp();
        EquiJoinPredicateList cdPreds = cdJoin.getEquiJoinPredicates();

        // New predicates
        EquiJoinPredicateList acbdPreds = new EquiJoinPredicateList();
        EquiJoinPredicateList acPreds = new EquiJoinPredicateList();
        EquiJoinPredicateList bdPreds = new EquiJoinPredicateList();

        // Gather all equijoin predicates
        EquiJoinPredicateList allPreds = abcdPreds.copy();
        allPreds.addAll(abPreds);
        allPreds.addAll(cdPreds);

        Attrs leftAttrs = allPreds.getLeft();
        Attrs rightAttrs = allPreds.getRight();
        HashMap attr2class = allPreds.getEquivalenceClasses();

        // Now distribute old predicates
        // AC and BD predicates go to their respective lower joins
        // AB, AD, BC, and CD go to the upper join, and if we can
        // find an equivalent attribute we push it to one of the lower joins as well
        forallpreds : for (int i = 0; i < leftAttrs.size(); i++) {
            Attribute la = leftAttrs.get(i);
            Attribute ra = rightAttrs.get(i);
            if (aAttrs.Contains(la) && cAttrs.Contains(ra))
                acPreds.add(la, ra);
            else if (bAttrs.Contains(la) && dAttrs.Contains(ra))
                bdPreds.add(la, ra);
            else {
                acbdPreds.add(la, ra);

                HashSet leqClass = (HashSet) attr2class.get(la);
                HashSet reqClass = (HashSet) attr2class.get(ra);

                // Try to push it to the lower joins
                Attrs checkLeft, checkRight;
                if (aAttrs.Contains(la) && bAttrs.Contains(ra)) {
                    checkLeft = cAttrs;
                    checkRight = dAttrs;
                } else if (aAttrs.Contains(la) && dAttrs.Contains(ra)) {
                    checkLeft = cAttrs;
                    checkRight = bAttrs;
                } else if (bAttrs.Contains(la) && cAttrs.Contains(ra)) {
                    checkLeft = dAttrs;
                    checkRight = aAttrs;
                } else {
                    assert cAttrs.Contains(la) && dAttrs.Contains(ra);
                    checkLeft = aAttrs;
                    checkRight = bAttrs;
                }
                Attribute z = findEquivalent(checkLeft, reqClass);
                if (z != null)
                    acPreds.add(la, z);
                z = findEquivalent(checkRight, leqClass);
                if (z != null)
                    bdPreds.add(z, ra);
            }
        }

        // Check that we're not producing a cartesian product if it's not allowed
        if (!ruleSet.allowCartesian()
            && !abJoin.isCartesian()
            && !cdJoin.isCartesian()
            && !abcdJoin.isCartesian()
            && (acPreds.isEmpty() || bdPreds.isEmpty() || acbdPreds.isEmpty()))
            return null;

        // Make sure that we have not created unnecessary predicates
        acPreds.removeDuplicates(attr2class);
        bdPreds.removeDuplicates(attr2class);
        acbdPreds.removeDuplicates(attr2class);

        // Now we have to deal with non-equijoin predicates
        // XXX vpapad: can we use equivalence classes to
        // push some of these to the lower joins???
        Predicate allNonEquiPreds =
            And.conjunction(
                abcdJoin.getNonEquiJoinPredicate(),
                And.conjunction(
                    abJoin.getNonEquiJoinPredicate(),
                    cdJoin.getNonEquiJoinPredicate()));
        Attrs ac = aAttrs.copy();
        ac.merge(cAttrs);
        Attrs bd = bAttrs.copy();
        bd.merge(dAttrs);
        And split = (And) allNonEquiPreds.split(ac);
        Predicate acPred = split.getLeft();
        Predicate rest = split.getRight();
        And split2 = (And) rest.split(bd);
        Predicate bdPred = split2.getLeft();
        Predicate acbdPred = split2.getRight();

        // KT - we can lose extension join property during association
        // no good way to know if new joins are extension joins or not
        // extension join is only optimization issue, not correctness issue
        // anyway. In future, should examine catalogs to see if these
        // are extension joins
        joinOp acJoin = new joinOp(acPred, acPreds, joinOp.NONE);
        joinOp bdJoin = new joinOp(bdPred, bdPreds, joinOp.NONE);
        joinOp acbdJoin = new joinOp(acbdPred, acbdPreds, joinOp.NONE);

        return new Expr(
            acbdJoin,
            new Expr(acJoin, new Expr(A), new Expr(C)),
            new Expr(bdJoin, new Expr(B), new Expr(D)));
    }

    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        // XXX vpapad: Columbia comment below
        // If we do not allow a non-Cartesian product to go to a Cartesian product
        // Need to check:
        //      a. Whether original  joins       contained a cartesian product
        //      b. Whether the new joins would contain   a cartesian product
        // Condition is:  a || !b

        // We will perform the real check in nextSubstitute
        return true;
    }

    /** Find an attribute in <code>schema</code> (if any) that 
     * belongs in a given equivalence class */
    public static Attribute findEquivalent(Attrs schema, HashSet eqClass) {
        Iterator it = eqClass.iterator();
        while (it.hasNext()) {
            Attribute z = (Attribute) it.next();
            if (schema.Contains(z)) // Bingo!
                return z;
        }
        return null;
    }
}
