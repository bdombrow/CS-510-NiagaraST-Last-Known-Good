import junit.framework.*;

import niagara.connection_server.Catalog;

public class TestCatalog extends TestCase {
    public TestCatalog(String name) {
        super(name);
    }

    public void testParsing() {
        Catalog c = new Catalog("sample_catalog.xml");
        c.dumpStats();
    }

    public static void main(String args[]) {
        junit.textui.TestRunner.run(new TestSuite(TestCatalog.class));
    }
}

