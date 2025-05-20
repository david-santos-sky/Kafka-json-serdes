package pt.dcs.kafka.schema;

import java.util.Map;

public class SerDesConfig {

    public static final String USE_SCHEMA_VERSION_ID_IN_HEADER = "gc.use.schema.version.id.in.header";
    public static final String DEFAULT_KEY_SCHEMA_VERSION_ID = "key.schema.version.id";
    public static final String DEFAULT_VALUE_SCHEMA_VERSION_ID = "value.schema.version.id";

    public static Boolean getOrDefaultAsBoolean(Map<String, ?> map, String key, Boolean defaultValue) {
        var value = (Boolean) map.get(key);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

}
