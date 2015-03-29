package umbc.hebeler.phd.bigdata;

import java.net.UnknownHostException;

import com.hp.hpl.jena.rdf.model.Model;
import com.mongodb.MongoClient;

public class ProbeMongoDB implements Probe {

	MongoClient mongoClient = null;

	public boolean connect(String url, String user, String pw) {
		try {
			 mongoClient = new MongoClient();
		} catch (UnknownHostException e) {
		 return false;
		}
		return true;
	}

	public boolean extractStructures() {
		// TODO Auto-generated method stub
		return false;
	}

	public Model convert2RDF() {
		// TODO Auto-generated method stub
		return null;
	}
}
