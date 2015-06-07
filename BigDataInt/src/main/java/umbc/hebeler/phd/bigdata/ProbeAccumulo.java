package umbc.hebeler.phd.bigdata;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

import com.hp.hpl.jena.rdf.model.Model;

public class ProbeAccumulo implements Probe {

	public boolean connect(String url, String user, String pw) {
		
		 Instance instances = new ZooKeeperInstance("accumulo", "accumuloServer");
		 AccumuloConfiguration ac = instances.getConfiguration();
		 AuthenticationToken token = new PasswordToken("123456789");
		 try {
			Connector conn = instances.getConnector("root",token);
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		return false;
	}

	public int extractStructures() {
		// TODO Auto-generated method stub
		return 1;
	}

	public Model convert2RDF() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean sameAs(Object first, Object second) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean populate() {
		// TODO Auto-generated method stub
		return false;
	}
}
