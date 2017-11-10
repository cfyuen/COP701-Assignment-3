package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.IntWritable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import common.Utils;

public class FilterSubmission {

	public static void main(String[] args) throws IOException {
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

		String str = "";
		while((str = input.readLine())!=null && str.length()!=0) {
			String json = str.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(json);
			ArrayNode result = (ArrayNode)node.get("result");
			
			String handle = "";
			
			for (JsonNode sub : result) {
				
				if (Utils.filterProblem(sub)) {
					handle = sub.get("author").get("members").get(0).get("handle").getTextValue();
					String problemName = sub.get("problem").get("name").getTextValue();
					ArrayNode tags = (ArrayNode) sub.get("problem").get("tags");
					String tagstr = "";
					for (JsonNode tag : tags) {
						if (tagstr.length() > 0) tagstr += ",";
						tagstr += tag.getTextValue();
					}
					
					System.out.println(handle + "\t" + problemName + "\t" + tagstr);
				}
			}
			
		}

	}

}
