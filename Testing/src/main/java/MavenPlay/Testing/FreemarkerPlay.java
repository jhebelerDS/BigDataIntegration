package MavenPlay.Testing;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class FreemarkerPlay {

	public static void main(String[] args) throws IOException, TemplateException {
		Configuration configuration = new Configuration();
		configuration.setClassForTemplateLoading(FreemarkerPlay.class, "/");
        
		Template helloT = configuration.getTemplate("hello.flt");
		StringWriter wr = new StringWriter();
		Map<String, Object> helloMap = new HashMap<String, Object> ();
		helloMap.put("name", "john");
		helloT.process(helloMap, wr);
		
		System.out.println(wr);
		
	}

}
