import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

public class wikidataQuery {
	
	public static void main(String[] args) {

		//SPAQRL endpoint
	    String endpoint = "http://query.wikidata.org/sparql";
	
	    //query >Q2283< durch beliebige Wikidata-ID ersetzen bzw. aus DB auslesen
	    String query =  "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"+
						"PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"+
						"PREFIX wd: <http://www.wikidata.org/entity/>\n"+
						"PREFIX wikibase: <http://wikiba.se/ontology#>\n"+
						"SELECT DISTINCT * WHERE {wd:Q2283 rdfs:label ?label. FILTER(langMatches(lang(?label), \"DE\"))}";                
	
	    //query execution
	    QueryExecution queryEx = QueryExecutionFactory.sparqlService(endpoint, query);
	
	    try {
	        //result
	        ResultSet results = queryEx.execSelect();
	
	        //output
	        for(; results.hasNext();){
	            //typecast results from set to qsolution
	            QuerySolution answer = (QuerySolution)results.next();
	
	            System.out.println(answer.get("?label"));       
	
	        }
	    } catch(Exception e){
	
	        e.printStackTrace();
	    } finally{
	        queryEx.close();
	    }
	}
}
