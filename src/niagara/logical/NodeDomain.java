/* $Id: NodeDomain.java,v 1.1 2002/09/20 23:21:38 vpapad Exp $ */

package niagara.logical;

import niagara.optimizer.colombia.ATTR;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Domain;
import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.varType;

/**
 * A domain representing Domain nodes
 */
public class NodeDomain extends Domain {
    // XXX vpapad: we also need domains for strings and primitive types
    // for example the result of count should be an integer, not a NodeDomain
    
    private static NodeDomain elementDomNode;
    private static NodeDomain tagDomNode;
    private static NodeDomain contentDomNode;

    private int type;
    
    public String getTypeDescription() {
        switch (type) {
            case varType.ELEMENT_VAR:
                return "element";
            case varType.TAG_VAR:
                return "tag";
            case varType.CONTENT_VAR:
                return "content";
            default:
                throw new PEException("Unexpected variable type");
        }
    }

    public int getType() {
        return type;
    }
            
    static {
        elementDomNode = new NodeDomain(varType.ELEMENT_VAR);
        tagDomNode = new NodeDomain(varType.TAG_VAR);
        contentDomNode = new NodeDomain(varType.CONTENT_VAR);        
    }
    
    
    public static NodeDomain getDOMNode() {
        return elementDomNode;
    }
    
    public static NodeDomain getDOMNode(int type) {
        switch (type) {
            case varType.ELEMENT_VAR:
                return elementDomNode;
            case varType.TAG_VAR:
                return tagDomNode;
            case varType.CONTENT_VAR:
                return contentDomNode;
            default:
                throw new PEException("Unexpected variable type");
        }
    }
    
    // private constructor, NodeDomain is a singleton
    private NodeDomain(int type) {
        super("NodeDomain");
        this.type = type;
    }
}
