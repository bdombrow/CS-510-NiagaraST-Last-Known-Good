/* $Id$ */
package niagara.optimizer.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import niagara.optimizer.colombia.*;
import niagara.logical.*;
import niagara.xmlql_parser.op_tree.joinOp;

/** Join associativity, right to left: A x (B x C) -> (A x B) x C */
public class AssociateJoinRtoL extends CustomRule {

    public AssociateJoinRtoL(String name) {
        super(
            name,
            3,
            new Expr(
                new joinOp(),
                new Expr(new LeafOp(0)),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(1)),
                    new Expr(new LeafOp(2)))),
            new Expr(
                new joinOp(),
                new Expr(
                    new joinOp(),
                    new Expr(new LeafOp(0)),
                    new Expr(new LeafOp(1))),
                new Expr(new LeafOp(2))));
    }

    public Expr next_substitute(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        // Get schema for B 
        Expr BC = before.getInput(1);
        LeafOp B = (LeafOp) (BC.getInput(0).getOp());
        Attrs bAttrs = B.getGroup().getLogProp().getAttrs();

        // Get C's schema
        LeafOp C = (LeafOp) (BC.getInput(1).getOp());
        Attrs cAttrs = C.getGroup().getLogProp().getAttrs();

        // Get A's schema
        LeafOp A = (LeafOp) (before.getInput(0).getOp());
        Attrs aAttrs = A.getGroup().getLogProp().getAttrs();

         // Join numbering convention:
         //         2  1               1  2		
         //       Ax(BxC) ->        (AxB)xC
        
        // from upper (second) join
        joinOp op2 = (joinOp) before.getOp();
        EquiJoinPredicateList preds2 = op2.getEquiJoinPredicates();

        // from lower (first) join
        joinOp op1 = (joinOp) before.getInput(1).getOp();
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
        // AB predicates go to the new lower join (1)
        // A BC predicate B.x = C.y goes to  (2),
        // and if we can find an A attribute A.z equivalent 
        // to C.y, we also add A.z = B.x to (1)
        forallpreds : for (int i = 0; i < leftAttrs.size(); i++) {
            Attribute la = leftAttrs.get(i);
            Attribute ra = rightAttrs.get(i);
            if (aAttrs.Contains(la)) {
                if (bAttrs.Contains(ra))
                    newPreds1.add(la, ra);
                else {
                    assert cAttrs.Contains(ra);
                    newPreds2.add(la, ra);
                }
            } else {
                assert bAttrs.Contains(la) && cAttrs.Contains(ra);
                newPreds2.add(la, ra);
                HashSet eqClass = (HashSet) attr2class.get(ra);
                Iterator it = eqClass.iterator();
                while (it.hasNext()) {
                    Attribute az = (Attribute) it.next();
                    if (aAttrs.Contains(az)) { // Bingo!
                        newPreds1.add(az, la);
                        continue forallpreds;
                    }
                }
            }
        }

        // Make sure that we have not created unnecessary predicates
        newPreds1.removeDuplicates(attr2class);
        newPreds2.removeDuplicates(attr2class);

        // Now we have to deal with non-equijoin predicates
        // XXX vpapad: can we use equivalence classes to
        // push some of these to AB???
        Attrs ab = aAttrs.copy();
        ab.merge(bAttrs);
        And split2 = (And) op2.getNonEquiJoinPredicate().split(ab);
        Predicate abPred = split2.getLeft();
	// KT - we can lose extension join property during association
	// no good way to know if new joins are extension joins or not
	// extension join is only optimization issue, not correctness issue
	// anyway. In future, should examine catalogs to see if these
	// are extension joins
        joinOp newOp1 = new joinOp(abPred, newPreds1,joinOp.NONE);
        Predicate abcPred =
            And.conjunction(split2.getRight(), op1.getNonEquiJoinPredicate());
        joinOp newOp2 = new joinOp(abcPred, newPreds2,joinOp.NONE);

        // Phew!
        return new Expr(
            newOp2,
            new Expr(newOp1, new Expr(A), new Expr(B)),
            new Expr(C));
    }

    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        // XXX vpapad: Columbia comment below
        // If we do not allow a non-Cartesian product to go to a Cartesian product
        // Need to check:
        //      a. Whether original 2 joins       contained a cartesian product
        //      b. Whether the new  2 joins would contain   a cartesian product
        // Condition is:  a || !b

        if (ruleSet.allowCartesian()) {
            joinOp op2 = (joinOp) before.getOp();
            joinOp op1 = (joinOp) before.getInput(1).getOp();
            if (op1.isCartesian() || op2.isCartesian())
                return true;
            // Get schema for B 
            Expr BC = before.getInput(1);
            LeafOp B = (LeafOp) (BC.getInput(0).getOp());
            Attrs bAttrs = B.getGroup().getLogProp().getAttrs();

            // Get C's schema
            LeafOp C = (LeafOp) (BC.getInput(1).getOp());
            Attrs cAttrs = C.getGroup().getLogProp().getAttrs();

            // Get A's schema
            LeafOp A = (LeafOp) (before.getInput(0).getOp());
            Attrs aAttrs = A.getGroup().getLogProp().getAttrs();

            EquiJoinPredicateList allPreds = op2.getEquiJoinPredicates().copy();
            allPreds.addAll(op1.getEquiJoinPredicates());
            HashMap attr2class = allPreds.getEquivalenceClasses();
            Attrs left = allPreds.getLeft();
            Attrs right = allPreds.getRight();

            // We only need to check that join (1) is not cartesian
            for (int i = 0; i < allPreds.size(); i++) {
                Attribute la = left.get(i);
                if (!aAttrs.Contains(la))
                    continue;
                Attribute ra = right.get(i);
                if (bAttrs.Contains(ra))
                    return true;
                assert cAttrs.Contains(ra);
                HashSet eqclass = (HashSet) attr2class.get(ra);
                Iterator iter = eqclass.iterator();
                while (iter.hasNext()) {
                    Attribute eq = (Attribute) iter.next();
                    if (bAttrs.Contains(eq))
                        return true;
                }
            }

            // We cannot assign any predicates to new join (1)
            return false;
        }
        // otherwise just return true
        return true;
    }
}
