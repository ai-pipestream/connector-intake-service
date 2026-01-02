package ai.pipeline.connector.intake.util;

import org.apache.commons.validator.routines.UrlValidator;
import org.jboss.logging.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Utility for canonicalizing URIs to create stable document IDs.
 * <p>
 * Transforms URIs into a normalized form that maximizes collision between
 * different strings pointing to the same resource. Used for deterministic
 * doc_id generation.
 * <p>
 * Canonicalization rules:
 * <ul>
 *   <li>Validates URI syntax using Apache Commons Validator</li>
 *   <li>Normalizes path navigation (../, ./)</li>
 *   <li>Lowercases scheme and host only (preserves case in path/query)</li>
 *   <li>Removes trailing slashes (except root "/")</li>
 *   <li>Sorts query parameters alphabetically</li>
 *   <li>Removes fragments (client-side only)</li>
 *   <li>Normalizes default ports (:80, :443)</li>
 * </ul>
 */
public class UriCanonicalizer {

    private static final Logger LOG = Logger.getLogger(UriCanonicalizer.class);

    /**
     * Apache Commons Validator 1.10.1 for URI validation.
     * Allows http and https protocols, including local URLs for development.
     */
    private static final UrlValidator VALIDATOR = new UrlValidator(
        new String[]{"http", "https"},
        UrlValidator.ALLOW_LOCAL_URLS
    );

    /**
     * Transforms a source URI into a canonical ID string.
     * <p>
     * The canonical form is designed to maximize collision between different
     * URI strings that represent the same resource, while preserving case
     * sensitivity where it matters (paths, query parameters).
     *
     * @param rawUri The input source URI
     * @param keepQueryParams If false, strips query parameters for a "base resource" ID
     * @return A normalized string suitable for use as a unique identifier
     * @throws IllegalArgumentException if the URI is invalid or cannot be parsed
     */
    public static String canonicalizeUri(String rawUri, boolean keepQueryParams) {
        if (rawUri == null || rawUri.isBlank()) {
            throw new IllegalArgumentException("URI source cannot be empty");
        }

        // 1. Structural Validation
        String trimmed = rawUri.trim();
        if (!VALIDATOR.isValid(trimmed)) {
            LOG.warnf("Invalid URI format (will attempt parse anyway): %s", trimmed);
            // Continue with parsing - validator is strict, but java.net.URI is more lenient
        }

        try {
            // 2. Leverage JDK URI normalization (handles path navigation like ../, ./)
            URI uri = new URI(trimmed).normalize();

            // 3. Reconstruct with logic-specific rules
            String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : "http";
            String host = uri.getHost() != null ? uri.getHost().toLowerCase() : "";
            int port = uri.getPort();

            // Normalize default ports (remove :80 for http, :443 for https)
            if (port == -1 || (scheme.equals("http") && port == 80) || (scheme.equals("https") && port == 443)) {
                port = -1; // -1 means use default port in URI constructor
            }

            // 4. Handle Path: remove trailing slash unless it's the root "/"
            String path = uri.getPath();
            if (path != null && path.length() > 1 && path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            if (path == null || path.isEmpty()) {
                path = "/";
            }

            // 5. Handle Query Parameters (Alphabetical sorting for consistency)
            String sortedQuery = null;
            if (keepQueryParams && uri.getQuery() != null && !uri.getQuery().isEmpty()) {
                sortedQuery = sortQueryParameters(uri.getQuery());
            }

            // 6. Build final string (Fragments ignored - they are client-side only)
            URI canonical = new URI(scheme, null, host, port, path, sortedQuery, null);
            return canonical.toASCIIString(); // Ensures US-ASCII encoding with percent-encoding for Unicode

        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to parse URI: " + rawUri + " - " + e.getMessage(), e);
        }
    }

    /**
     * Sorts query parameters alphabetically by parameter name.
     * <p>
     * This ensures that different orderings of the same parameters produce
     * the same canonical form. For example:
     * - {@code ?a=1&b=2} → {@code ?a=1&b=2}
     * - {@code ?b=2&a=1} → {@code ?a=1&b=2}
     *
     * @param query The raw query string (e.g., "a=1&b=2")
     * @return The sorted query string
     */
    private static String sortQueryParameters(String query) {
        return Arrays.stream(query.split("&"))
                .sorted()
                .collect(Collectors.joining("&"));
    }

    /**
     * Canonicalizes a URI, keeping query parameters.
     * Convenience method that calls {@link #canonicalizeUri(String, boolean)} with keepQueryParams=true.
     *
     * @param rawUri The input source URI
     * @return The canonicalized URI string
     */
    public static String canonicalizeUri(String rawUri) {
        return canonicalizeUri(rawUri, true);
    }
}


