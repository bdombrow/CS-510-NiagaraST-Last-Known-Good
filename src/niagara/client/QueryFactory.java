package niagara.client;

public class QueryFactory {

    public static Query makeQuery(String text) {
	   if (text.indexOf("CREATE TRIGGER") != -1) 
	       return new TriggerQuery(text);
	   else if (text.startsWith("<?xml")) 
	       return new QPQuery(text);
	   else if (text.toUpperCase().indexOf("WHERE") != -1) 
	       return new XMLQLQuery(text);
	   else 
	       return new SEQuery(text);
    }
}
