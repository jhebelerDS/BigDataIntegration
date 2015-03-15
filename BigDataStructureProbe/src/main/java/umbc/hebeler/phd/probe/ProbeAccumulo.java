package umbc.hebeler.phd.probe;


import com.hp.hpl.jena.rdf.model.Model;

public class ProbeAccumulo implements Probe {

	public boolean connect(String url, String user, String pw) {
		
		 Instance instances = new ZooKeeperInstance("accumulo", "accumuloServer");
		 AccumuloConfiguration ac = instances.getConfiguration();
		 AuthenticationToken token = new PasswordToken("123456789");
		 Connector conn = instances.getConnector("root",token);
		 
		return false;
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
