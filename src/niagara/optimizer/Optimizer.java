/**
 * $Id: Optimizer.java,v 1.1 2002/05/23 06:31:23 vpapad Exp $
 */
package niagara.optimizer;

import niagara.xmlql_parser.op_tree.logNode;
import niagara.connection_server.NiagraServer;
import niagara.connection_server.Catalog;

public class Optimizer {
    public static void init() {
        System.loadLibrary("columbia");
        initColumbia(NiagraServer.getCatalog());
    }
    private native static void initColumbia(Catalog catalog);
    public native static logNode optimize(logNode plan);
}
