package umbc.hebeler.phd.bigdata;

import com.hp.hpl.jena.rdf.model.Model;

public interface Probe {
	   public boolean connect(String url, String user, String pw);
	   public boolean extractStructures();
	   public Model convert2RDF();
}
