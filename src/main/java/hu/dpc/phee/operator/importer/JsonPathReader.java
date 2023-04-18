package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.*;
import org.apache.commons.text.StringEscapeUtils;

public class JsonPathReader {
    private static ParseContext jsonParser;

    static {
        Configuration config = Configuration.defaultConfiguration()
                .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .addOptions(Option.SUPPRESS_EXCEPTIONS);
        jsonParser = JsonPath.using(config);
    }

    public static DocumentContext parse(String json) {
        return jsonParser.parse(json);
    }

    public static DocumentContext parseEscaped(String escapedJson) {
        String rawString = StringEscapeUtils.unescapeJson(strip(escapedJson));
        return jsonParser.parse(rawString);
    }

    public static String strip(String str) {
        return str.replaceAll("^\"|\"$", "");
    }

}
