package ai.pipeline.connector.intake.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for parsing and serializing connector metadata JSON.
 * <p>
 * Connector metadata is stored as JSON in the database and contains:
 * <ul>
 *   <li>S3 bucket and base path</li>
 *   <li>File size limits</li>
 *   <li>Rate limits</li>
 *   <li>Default metadata to add to all documents</li>
 * </ul>
 */
public class ConnectorMetadata {

    private String s3Bucket = "";
    private String s3BasePath = "";
    private long maxFileSize = 100 * 1024 * 1024; // 100MB default
    private long rateLimitPerMinute = 1000; // 1000 docs/min default
    private Map<String, String> defaultMetadata = new HashMap<>();

    /**
     * Create an instance with safe defaults.
     * <ul>
     *   <li>s3Bucket/s3BasePath: empty strings</li>
     *   <li>maxFileSize: 100 MiB</li>
     *   <li>rateLimitPerMinute: 1000</li>
     *   <li>defaultMetadata: empty map</li>
     * </ul>
     */
    public ConnectorMetadata() {}

    /**
     * Get the S3 bucket name used for storage.
     *
     * @return bucket name (never null, may be empty)
     */
    public String getS3Bucket() {
        return s3Bucket;
    }

    /**
     * Set the S3 bucket name used for storage.
     *
     * @param s3Bucket bucket name; null is treated as empty string
     */
    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket != null ? s3Bucket : "";
    }

    /**
     * Get the base path within the S3 bucket where documents are stored.
     *
     * @return base path/prefix (never null, may be empty)
     */
    public String getS3BasePath() {
        return s3BasePath;
    }

    /**
     * Set the base path within the S3 bucket where documents are stored.
     *
     * @param s3BasePath path/prefix; null is treated as empty string
     */
    public void setS3BasePath(String s3BasePath) {
        this.s3BasePath = s3BasePath != null ? s3BasePath : "";
    }

    /**
     * Get the maximum allowed file size for uploads.
     *
     * @return max size in bytes
     */
    public long getMaxFileSize() {
        return maxFileSize;
    }

    /**
     * Set the maximum allowed file size for uploads.
     *
     * @param maxFileSize size in bytes; non-positive values reset to default (100 MiB)
     */
    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize > 0 ? maxFileSize : 100 * 1024 * 1024;
    }

    /**
     * Get the allowed ingestion rate.
     *
     * @return documents per minute
     */
    public long getRateLimitPerMinute() {
        return rateLimitPerMinute;
    }

    /**
     * Set the allowed ingestion rate.
     *
     * @param rateLimitPerMinute documents per minute; non-positive values reset to default (1000)
     */
    public void setRateLimitPerMinute(long rateLimitPerMinute) {
        this.rateLimitPerMinute = rateLimitPerMinute > 0 ? rateLimitPerMinute : 1000;
    }

    /**
     * Get default metadata applied to every stored document.
     *
     * @return map of key-value metadata (never null)
     */
    public Map<String, String> getDefaultMetadata() {
        return defaultMetadata;
    }

    /**
     * Set default metadata applied to every stored document.
     *
     * @param defaultMetadata map of key-value metadata; null becomes an empty map
     */
    public void setDefaultMetadata(Map<String, String> defaultMetadata) {
        this.defaultMetadata = defaultMetadata != null ? defaultMetadata : new HashMap<>();
    }

    /**
     * Convert this metadata object to its JSON representation.
     *
     * @return JSON string encoding the connector metadata
     */
    public String toJson() {
        Gson gson = new Gson();
        JsonObject json = new JsonObject();
        json.addProperty("s3Bucket", s3Bucket);
        json.addProperty("s3BasePath", s3BasePath);
        json.addProperty("maxFileSize", maxFileSize);
        json.addProperty("rateLimitPerMinute", rateLimitPerMinute);
        
        if (!defaultMetadata.isEmpty()) {
            JsonObject metadataJson = new JsonObject();
            for (Map.Entry<String, String> entry : defaultMetadata.entrySet()) {
                metadataJson.addProperty(entry.getKey(), entry.getValue());
            }
            json.add("defaultMetadata", metadataJson);
        }
        
        return gson.toJson(json);
    }

    /**
     * Parse a ConnectorMetadata instance from its JSON representation.
     * Unknown fields are ignored. On parse errors, a default instance is returned.
     *
     * @param json JSON string produced by {@link #toJson()} or equivalent structure
     * @return parsed {@code ConnectorMetadata} instance; default instance if input is null/empty/invalid
     */
    public static ConnectorMetadata fromJson(String json) {
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return new ConnectorMetadata();
        }

        try {
            Gson gson = new Gson();
            JsonObject obj = gson.fromJson(json, JsonObject.class);
            
            ConnectorMetadata metadata = new ConnectorMetadata();
            
            if (obj.has("s3Bucket")) {
                metadata.setS3Bucket(obj.get("s3Bucket").getAsString());
            }
            if (obj.has("s3BasePath")) {
                metadata.setS3BasePath(obj.get("s3BasePath").getAsString());
            }
            if (obj.has("maxFileSize")) {
                metadata.setMaxFileSize(obj.get("maxFileSize").getAsLong());
            }
            if (obj.has("rateLimitPerMinute")) {
                metadata.setRateLimitPerMinute(obj.get("rateLimitPerMinute").getAsLong());
            }
            if (obj.has("defaultMetadata")) {
                JsonObject metadataObj = obj.getAsJsonObject("defaultMetadata");
                Map<String, String> defaultMetadata = new HashMap<>();
                for (String key : metadataObj.keySet()) {
                    defaultMetadata.put(key, metadataObj.get(key).getAsString());
                }
                metadata.setDefaultMetadata(defaultMetadata);
            }
            
            return metadata;
        } catch (Exception e) {
            // Return default metadata on parse error
            return new ConnectorMetadata();
        }
    }
}
