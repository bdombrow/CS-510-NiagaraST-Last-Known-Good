/* $Id: AssociateJoinLtoR.java,v 1.1 2003/09/13 03:44:02 vpapad Exp $ */
package niagara.optimizer.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import niagara.optimizer.colombia.*;
import niagara.logical.*;
import niagara.xmlql_parser.op_tree.joinOp;

/** Join associativity, left to right : (A x B) x C -> A x (B x C) */
public class AssociateJoinLtoR extends CustomRule {
    public AssociateJoinLtoR(String name) {
        super(
            name,
            3,
            new Expr(
                new joinOp(),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(0)),
                    new Expr(new LeafOp(1))),
                new Expr(new LeafOp(2))),
            new Expr(
                new joinOp(),
                new Expr(new LeafOp(0)),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(1)),
                    new Expr(new LeafOp(2)))));
    }

    public Expr nextSubstitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        // Get A's schema 
        Expr AB = before.getInput(0);
        LeafOp A = (LeafOp) (AB.getInput(0).getOp());
        Attrs aAttrs = A.getGroup().getLogProp().getAttrs();

        // Get B's schema
        LeafOp B = (LeafOp) (AB.getInput(1).getOp());
        Attrs bAttrs = B.getGroup().getLogProp().getAttrs();

        // Get C's schema
        LeafOp C = (LeafOp) (before.getInput(1).getOp());
        Attrs cAttrs = C.getGroup().getLogProp().getAttrs();

        // Join numbering convention: 
        // before: AxB is join 1, ABxC is join 2
        // after:   BC is join 1, AxBC is join 2

        // from upper (second) join
        joinOp op2 = (joinOp) before.getOp();
        EquiJoinPredicateList preds2 = op2.getEquiJoinPredicates();

        // from lower (first) join
        joinOp op1 = (joinOp) before.getInput(0).getOp();
        EquiJoinPredicateList preds1 = op1.getEquiJoinPredicates();

        // new equijoin predicates
        EquiJoinPredicateList newPreds1 = new EquiJoinPredicateList();
        EquiJoinPredicateList newPreds2 = new EquiJoinPredicateList();

        EquiJoinPredicateList allPreds = preds1.copy();
        allPreds.addAll(preds2);
        Attrs leftAttrs = allPreds.getLeft();
        Attrs rightAttrs = allPreds.getRight();
        HashMap attr2class = allPreds.getEquivalenceClasses();

        // Now distribute old predicates
        // AC predicates go to the new top join (2)
        // BC predicates go to the new lower join (1)
        // An AB predicate A.x = B.y goes to  (2),
        // and if we can find a C attribute C.z equivalent 
        // to A.x, we also add B.y = C.z to (1)
        forallpreds : for (int i = 0; i < leftAttrs.size(); i++) {
            Attribute la = leftAttrs.get(i);
            Attribute ra = rightAttrs.get(i);
            if (cAttrs.Contains(ra)) {
                if (bAttrs.Contains(la))
                    newPreds1.add(la, ra);
                else {
                    assert aAttrs.Contains(la);
                    newPreds2.add(la, ra);
                }
            } else {
                assert aAttrs.Contains(la) && bAttrs.Contains(ra);
                newPreds2.add(la, ra);
                HashSet eqClass = (HashSet) attr2class.get(la);
                Iterator it = eqClass.iterator();
                while (it.hasNext()) {
                    Attribute cz = (Attribute) it.next();
                    if (cAttrs.Contains(cz)) { // Bingo!
                        newPreds1.add(ra, cz);
                        continue forallpreds;
                    }
                }
            }
        }

        // Check that we're not producing a cartesian product if it's not allowed
        if (!ruleSet.allowCartesian()
            && !op1.isCartesian()
            && !op2.isCartesian()
            && (newPreds1.isEmpty() || newPreds2.isEmpty()))
            return null;

        // Make sure that we have not created unnecessary predicates
        newPreds1.removeDuplicates(attr2class);
        newPreds2.removeDuplicates(attr2class);

        // Now we have to deal with non-equijoin predicates
        // XXX vpapad: can we use equivalence classes to
        // push some of these to BC???
        Attrs bc = bAttrs.copy();
        bc.merge(cAttrs);
        And split2 = (And) op2.getNonEquiJoinPredicate().split(bc);
        Predicate bcPred = split2.getLeft();
        // KT - we can lose extension join property during association
        // no good way to know if new joins are extension joins or not
        // extension join is only optimization issue, not correctness issue
        // anyway. In future, should examine catalogs to see if these
        // are extension joins
        joinOp newOp1 = new joinOp(bcPred, newPreds1, joinOp.NONE);
        Predicate abcPred =
            And.conjunction(split2.getRight(), op1.getNonEquiJoinPredicate());
        joinOp newOp2 = new joinOp(abcPred, newPreds2, joinOp.NONE);

        // Phew!
        return new Expr(
            newOp2,
            new Expr(A),
            new Expr(newOp1, new Expr(B), new Expr(C)));
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
}
