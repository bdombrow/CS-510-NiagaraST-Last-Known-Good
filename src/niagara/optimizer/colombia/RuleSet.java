/* $Id: RuleSet.java,v 1.2 2002/12/10 01:18:26 vpapad Exp $ */
package niagara.optimizer.colombia;

import java.util.ArrayList;
import java.util.HashMap;

/** A RuleSet is a container of rules */
public class RuleSet {
    private ArrayList rules;
    private SSP ssp;
    
    // XXX vpapad: UNIQ means the opposite of what it meant in Columbia
    // ... although the comment there matches this semantics!
    /** Is this a ruleset that never generates duplicate expressions? */
    private boolean unique;
    
    /** Does this ruleset allow the generation of cartesian products? */
    private boolean allowCartesian = false;


    private HashMap name2index;
    
        
    public RuleSet(ArrayList rules, boolean unique) {
        this.rules = rules;
        this.unique = unique;
        name2index = new HashMap(rules.size());
        
        for (int i = 0; i < rules.size(); i++) {
            // XXX vpapad: Should we copy the rule here?
            Rule r = (Rule) rules.get(i);
            r.setRuleSet(this);
            r.setIndex(i);
            name2index.put(r.GetName(), new Integer(i));
        }
        
        // Now initialize the rules
        for (int i = 0; i < rules.size(); i++) {
            Rule r = (Rule) rules.get(i);
            r.initMasks(rules.size());
            r.initialize();
        }
    }
    
    public int size() {
        return rules.size();
    }
    
    public boolean isUnique() {
        return unique;
    }
    
    public void setSSP(SSP ssp) {
        this.ssp = ssp;
    }
    
    public Rule get(int index) {
        return (Rule) rules.get(index);
    }
    
    public int getIndex(String name) {
        Integer i = (Integer) name2index.get(name);
        if (i == null) return -1;
        else return i.intValue();
    }
    
    public boolean allowCartesian() {
        return allowCartesian;
    }
}
