package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class JsonPathReader {
    private ParseContext jsonParser;

    @PostConstruct
    public void setup() {
        Configuration config = Configuration.defaultConfiguration()
                .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .addOptions(Option.SUPPRESS_EXCEPTIONS);
        jsonParser = JsonPath.using(config);
    }

    public DocumentContext parse(String json) {
        return jsonParser.parse(json);
    }
}
