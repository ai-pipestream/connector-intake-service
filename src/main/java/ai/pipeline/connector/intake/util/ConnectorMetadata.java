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

    public ConnectorMetadata() {}

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket != null ? s3Bucket : "";
    }

    public String getS3BasePath() {
        return s3BasePath;
    }

    public void setS3BasePath(String s3BasePath) {
        this.s3BasePath = s3BasePath != null ? s3BasePath : "";
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize > 0 ? maxFileSize : 100 * 1024 * 1024;
    }

    public long getRateLimitPerMinute() {
        return rateLimitPerMinute;
    }

    public void setRateLimitPerMinute(long rateLimitPerMinute) {
        this.rateLimitPerMinute = rateLimitPerMinute > 0 ? rateLimitPerMinute : 1000;
    }

    public Map<String, String> getDefaultMetadata() {
        return defaultMetadata;
    }

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
     * Parse from JSON string.
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
