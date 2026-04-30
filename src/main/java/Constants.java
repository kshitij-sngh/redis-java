import java.util.List;

public class Constants {

    public static final int DEFAULT_PORT = 6379;
    public static final List<String> WRITE_COMMANDS = List.of("SET", "INCR");
    public static final String STREAM_DELIMITER = "-";

    public static final String STREAM_ADD_ZERO_ERROR = "The ID specified in XADD must be greater than 0-0";
    public static final String STREAM_ADD_LESS_THAN_LAST_ERROR = "The ID specified in XADD is equal or smaller than the target stream top item";

    public static final String INC_KEY_NOT_INTEGER_ERROR = "value is not an integer or out of range";

    public static final String EXEC_WITHOUT_MULTI_ERROR = "EXEC without MULTI";

    public static final String DISCARD_WITHOUT_MULTI_ERROR = "DISCARD without MULTI";

    public static final String WATCH_INSIDE_MULTI_ERROR = "WATCH inside MULTI is not allowed";

    public static final String EMPTY_RDB_FILE_ENCODED = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
}
