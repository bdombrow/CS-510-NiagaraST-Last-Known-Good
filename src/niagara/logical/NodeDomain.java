/* $Id: NodeDomain.java,v 1.4 2003/08/01 17:28:45 tufte Exp $ */

package niagara.logical;

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
    private static NodeDomain nullDomain;

    private int type;
    
    public String getTypeDescription() {
        return varType.names[type];
    }

    public int getType() {
        return type;
    }
            
    static {
        elementDomNode = new NodeDomain(varType.ELEMENT_VAR);
        tagDomNode = new NodeDomain(varType.TAG_VAR);
        contentDomNode = new NodeDomain(varType.CONTENT_VAR);
        nullDomain = new NodeDomain(varType.NULL_VAR);        
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
                assert false : "Unexpected variable type " + type;
                return null;
        }
    }
    
    public static NodeDomain getDomain(int type) {
        switch (type) {
        case varType.ELEMENT_VAR:
        case varType.TAG_VAR:
        case varType.CONTENT_VAR:
            return getDOMNode(type);
        case varType.NULL_VAR:
            return nullDomain;
        default:
            assert false: "Unexpected type";
            return null;     
       
        }
    }
    
    // private constructor, NodeDomain is a singleton
    private NodeDomain(int type) {
        super("NodeDomain");
        this.type = type;
    }
}
