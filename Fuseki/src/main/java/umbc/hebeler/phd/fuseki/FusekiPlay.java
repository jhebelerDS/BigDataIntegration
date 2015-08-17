package umbc.hebeler.phd.fuseki;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.UUID;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

public class FusekiPlay {

		private static final String UPDATE_TEMPLATE = 
	            "PREFIX dc: <http://purl.org/dc/elements/1.1/>"
	            + "INSERT DATA"
	            + "{ <http://example/%s>    dc:title    \"A new book\" }";
		
	public static void main(String[] args) {
		
		Model m = ModelFactory.createDefaultModel();
		
		try {
		// Get current working environment
		FileInputStream ontology = new FileInputStream("/Users/jhebeler/Ontologies/NoSQLProbe.rdf");
	    m.read(ontology,"RDF/XML");
	} catch (FileNotFoundException e) {
		System.out.println("File Not Found");
		
	}
			
		// CHoice to use or combing multiple datasets such as fish below or graph adds as in line 45
        DatasetAccessor ds = DatasetAccessorFactory.createHTTP("http://localhost:3030/fish/data");
        //ds.add(m);
        ds.add("http://testds", m);
        // Query with graph
//        SELECT ?subject ?predicate ?object
//        		WHERE {
//        		GRAPH <http://testds>
//        		  {?subject ?predicate ?object}
//        		}
//        		LIMIT 25
//        
       
		String id = UUID.randomUUID().toString();
        System.out.println(String.format("Adding %s", id));
        UpdateProcessor upp = UpdateExecutionFactory.createRemote(
                UpdateFactory.create(String.format(UPDATE_TEMPLATE, id)), 
                "http://localhost:3030/ds/update");
        upp.execute();
        
        
//		UpdateRequest upr = UpdateFactory.create(UPDATE_TEMPLATE, "http://localhost:3030/ds/update");
//		
//		UpdateProcessor upp2 = UpdateExecutionFactory.createRemote(upr, "http://localhost:3030/ds/update");
//		
		
		String query = "Select ?x ?y ?z  where { ?x ?y ?z }";
		QueryExecution qe = QueryExecutionFactory.sparqlService("http://localhost:3030/new/query", query);
		
		ResultSet res = qe.execSelect();
		
		ResultSetFormatter.out(System.out, res);
		
		qe.close();

	}

}
