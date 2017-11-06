package common;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

public class Utils {

	public static boolean filterProblem(JsonNode sub) {
		if (sub.get("contestId").getIntValue() >= 100000) return false;
		if (!sub.get("verdict").getTextValue().equals("OK")) return false;
		ArrayNode tags = (ArrayNode) sub.get("problem").get("tags");
		boolean isSpecial = false;
		for (JsonNode tag : tags) {
			if (tag.getTextValue().equals("*special")) isSpecial = true;
		}
		if (isSpecial) return false;
		return true;
	}
}
