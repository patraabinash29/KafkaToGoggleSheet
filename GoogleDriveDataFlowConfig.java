public final class GcsSinkConfig extends AivenCommonConfig {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConfig.class);

    private static final String GROUP_GCS = "GCS";
    public static final String GCS_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    public static final String GCS_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";

    private static final String GROUP_FILE = "File";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";

    private static final String GROUP_FORMAT = "Format";
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";

    public static final String NAME_CONFIG = "name";

    private static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    public static ConfigDef configDef() {
        final GcsSinkConfigDef configDef = new GcsSinkConfigDef();
        addGcsConfigGroup(configDef);
        addFileConfigGroup(configDef);
        addFormatConfigGroup(configDef);
        return configDef;
    }

    private static void addGcsConfigGroup(final ConfigDef configDef) {
        int gcsGroupCounter = 0;
        configDef.define(
            GCS_CREDENTIALS_PATH_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "The path to a GCP credentials file. "
                + "If not provided, the connector will try to detect the credentials automatically. "
                + "Cannot be set together with \"" + GCS_CREDENTIALS_JSON_CONFIG + "\"",
            GROUP_GCS,
            gcsGroupCounter++,
            ConfigDef.Width.NONE,
            GCS_CREDENTIALS_PATH_CONFIG
        );

        configDef.define(
            GCS_CREDENTIALS_JSON_CONFIG,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.LOW,
            "GCP credentials as a JSON string. "
                + "If not provided, the connector will try to detect the credentials automatically. "
                + "Cannot be set together with \"" + GCS_CREDENTIALS_PATH_CONFIG + "\"",
            GROUP_GCS,
            gcsGroupCounter++,
            ConfigDef.Width.NONE,
            GCS_CREDENTIALS_JSON_CONFIG
        );

        configDef.define(
            GCS_BUCKET_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            "The GCS bucket name to store output files in.",
            GROUP_GCS,
            gcsGroupCounter++,
            ConfigDef.Width.NONE,
            GCS_BUCKET_NAME_CONFIG
        );
    }

    private static void addFileConfigGroup(final ConfigDef configDef) {
        int fileGroupCounter = 0;
        configDef.define(
            FILE_NAME_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    // See https://cloud.google.com/storage/docs/naming
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (valueStr.length() > 1024) {
                        throw new ConfigException(GCS_BUCKET_NAME_CONFIG, value,
                            "cannot be longer than 1024 characters");
                    }
                    if (valueStr.startsWith(".well-known/acme-challenge")) {
                        throw new ConfigException(GCS_BUCKET_NAME_CONFIG, value,
                            "cannot start with '.well-known/acme-challenge'");
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The prefix to be added to the name of each file put on GCS.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.NONE,
            FILE_NAME_PREFIX_CONFIG
        );

        configDef.define(
            FILE_NAME_TEMPLATE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG),
            ConfigDef.Importance.MEDIUM,
            "The template for file names on GCS. "
                + "Supports `{{ variable }}` placeholders for substituting variables. "
                + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                + "(the offset of the first record in the file). "
                + "Only some combinations of variables are valid, which currently are:\n"
                + "- `topic`, `partition`, `start_offset`.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.LONG,
            FILE_NAME_TEMPLATE_CONFIG
        );

        final String supportedCompressionTypes = CompressionType.names().stream()
            .map(f -> "'" + f + "'")
            .collect(Collectors.joining(", "));
        configDef.define(
            FILE_COMPRESSION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            CompressionType.NONE.name,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof String;
                    final String valueStr = (String) value;
                    if (!CompressionType.names().contains(valueStr)) {
                        throw new ConfigException(
                            FILE_COMPRESSION_TYPE_CONFIG, valueStr,
                            "supported values are: " + supportedCompressionTypes);
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The compression type used for files put on GCS. "
                + "The supported values are: " + supportedCompressionTypes + ".",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.NONE,
            FILE_COMPRESSION_TYPE_CONFIG,
            FixedSetRecommender.ofSupportedValues(CompressionType.names())
        );

        configDef.define(
            FILE_MAX_RECORDS,
            ConfigDef.Type.INT,
            0,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof Integer;
                    if ((Integer) value < 0) {
                        throw new ConfigException(
                            FILE_MAX_RECORDS, value,
                            "must be a non-negative integer number");
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The maximum number of records to put in a single file. "
                + "Must be a non-negative integer number. "
                + "0 is interpreted as \"unlimited\", which is the default.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_MAX_RECORDS
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_TIMEZONE,
            ConfigDef.Type.STRING,
            ZoneOffset.UTC.toString(),
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    try {
                        ZoneId.of(value.toString());
                    } catch (final Exception e) {
                        throw new ConfigException(
                            FILE_NAME_TIMESTAMP_TIMEZONE,
                            value,
                            e.getMessage());
                    }
                }
            },
            ConfigDef.Importance.LOW,
            "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                + "Use standard shot and long names. Default is UTC",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_TIMEZONE
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_SOURCE,
            ConfigDef.Type.STRING,
            TimestampSource.Type.WALLCLOCK.name(),
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    try {
                        TimestampSource.Type.of(value.toString());
                    } catch (final Exception e) {
                        throw new ConfigException(
                            FILE_NAME_TIMESTAMP_SOURCE,
                            value,
                            e.getMessage());
                    }
                }
            },
            ConfigDef.Importance.LOW,
            "Specifies the the timestamp variable source. Default is wall-clock.",
            GROUP_FILE,
            fileGroupCounter,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_SOURCE
        );

    }
