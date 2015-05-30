package umbc.hebeler.phd.bigdata;

import com.hp.hpl.jena.rdf.model.Model;

public interface Probe {
	   int MAXSAMPLES = 5;
	   int BATCHSIZE = 100;
	   public boolean connect(String url, String user, String pw);
	   public boolean extractStructures();
	   public Model convert2RDF();
	   public boolean sameAs (Object first, Object second);
	   public boolean populate();
}
