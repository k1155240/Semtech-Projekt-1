import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;

public class wikidataQuery {
	
	public static void main(String[] args) {

		//SPAQRL endpoint
	    String endpoint = "http://query.wikidata.org/sparql";
	
	    //query >Q2283< durch beliebige Wikidata-ID ersetzen bzw. aus DB auslesen
	    String query =  "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"+
						"PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"+
						"PREFIX wd: <http://www.wikidata.org/entity/>\n"+
						"PREFIX wikibase: <http://wikiba.se/ontology#>\n"+
						"PREFIX bd: <http://www.bigdata.com/rdf#>\n"+
						"SELECT DISTINCT ?item ?itemLabel ?industryLabel ?hqlocLabel ?countryLabel\n"+
						"WHERE"+
						"{"+
						  	"?item wdt:P31 wd:Q1058914 ;"+
						  	"wdt:P452 ?industry ;"+
						  	"wdt:P159 ?hqloc ;"+
						  	"wdt:P17 ?country ;"+
						  	"SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }"+
						"}"+
						"LIMIT 10";
	
	    //query execution
	    QueryExecution queryEx = QueryExecutionFactory.sparqlService(endpoint, query);
	
	    try {
	        //result
	        ResultSet results = queryEx.execSelect();
	        ResultSetFormatter.out(System.out, results) ;
	        //output
	        for(; results.hasNext();){
	            //typecast results from set to qsolution
	            QuerySolution answer = (QuerySolution)results.next();
	            
	            
	
	            System.out.println(answer.get("?item"));
	            System.out.println(answer.get("?itemLabel"));
	            System.out.println(answer.get("?industryLabel"));
	            System.out.println(answer.get("?hqlocLabel"));
	            System.out.println(answer.get("?country"));
	            System.out.println("\n");
	
	        }
	    } catch(Exception e){
	
	        e.printStackTrace();
	    } finally{
	        queryEx.close();
	    }
	}
}
