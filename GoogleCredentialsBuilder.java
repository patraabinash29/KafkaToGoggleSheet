public final class GoogleCredentialsBuilder {
    private static final Logger log = LoggerFactory.getLogger(GoogleCredentialsBuilder.class);
    
    public static GoogleCredentials build(final String credentialsPath,
                                          final String credentialsJson) throws IOException, IllegalArgumentException {
        if (credentialsPath != null && credentialsJson != null) {
            throw new IllegalArgumentException("Both credentialsPath and credentialsJson cannot be non-null.");
        }

        if (credentialsPath != null) {
            log.debug("Using provided credentials path");
            return getCredentialsFromPath(credentialsPath);
        }

        if (credentialsJson != null) {
            log.debug("Using provided credentials JSON");
            return getCredentialsFromJson(credentialsJson);
        }

        log.debug("Using default credentials");
        return GoogleCredentials.getApplicationDefault();
    }

    private static GoogleCredentials getCredentialsFromPath(final String credentialsPath) throws IOException {
        try (final InputStream stream = new FileInputStream(credentialsPath)) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            throw new IOException("Failed to read GCS credentials from " + credentialsPath, e);
        }
    }

    private static GoogleCredentials getCredentialsFromJson(final String credentialsJson) throws IOException {
        try (final InputStream stream = new ByteArrayInputStream(credentialsJson.getBytes())) {
            return GoogleCredentials.fromStream(stream);
        } catch (final IOException e) {
            throw new IOException("Failed to read credentials from JSON string", e);
        }
    }
}
