package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.util.UriCanonicalizer;
import ai.pipestream.data.v1.DocIdDerivation;
import ai.pipestream.data.v1.DocIdDerivationMethod;
import ai.pipestream.data.v1.SearchMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PipeDocIdDeriver {

    private static final Logger LOG = Logger.getLogger(PipeDocIdDeriver.class);

    /**
     * Deterministically derive a doc_id for a document.
     * <p>
     * Priority order (first match wins):
     * 1. Client-provided doc_id
     * 2. source_doc_id from request
     * 3. source_uri from search_metadata, canonicalized
     * 4. source_path from search_metadata, normalized
     */
    public DocIdDerivationResult derive(String datasourceId, String clientDocId, String sourceDocId,
                                        SearchMetadata searchMetadata) {
        if (clientDocId != null && !clientDocId.isBlank()) {
            return new DocIdDerivationResult(
                    clientDocId,
                    DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CLIENT_PROVIDED,
                    clientDocId);
        }

        if (sourceDocId != null && !sourceDocId.isBlank()) {
            String derivedId = datasourceId + ":" + sourceDocId;
            return new DocIdDerivationResult(
                    derivedId,
                    DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_DOC_ID,
                    sourceDocId);
        }

        if (searchMetadata != null && !searchMetadata.getSourceUri().isBlank()) {
            String canonicalUri = canonicalizeUri(searchMetadata.getSourceUri());
            String derivedId = datasourceId + ":" + canonicalUri;
            return new DocIdDerivationResult(
                    derivedId,
                    DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_URI,
                    searchMetadata.getSourceUri());
        }

        if (searchMetadata != null && !searchMetadata.getSourcePath().isBlank()) {
            String normalizedPath = normalizePath(searchMetadata.getSourcePath());
            String derivedId = datasourceId + ":" + normalizedPath;
            return new DocIdDerivationResult(
                    derivedId,
                    DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_PATH,
                    searchMetadata.getSourcePath());
        }

        return null;
    }

    private String canonicalizeUri(String uri) {
        if (uri == null || uri.isBlank()) {
            return null;
        }
        try {
            return UriCanonicalizer.canonicalizeUri(uri, true);
        } catch (IllegalArgumentException e) {
            LOG.warnf("Failed to canonicalize URI '%s': %s - using original", uri, e.getMessage());
            return uri.trim();
        }
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String normalized = path.trim().replace('\\', '/');
        normalized = normalized.replaceAll("/+", "/");
        normalized = normalized.replaceAll("^/", "");
        normalized = normalized.replaceAll("/$", "");
        return normalized;
    }

    public static class DocIdDerivationResult {
        public final String docId;
        public final DocIdDerivationMethod method;
        public final String input;

        DocIdDerivationResult(String docId, DocIdDerivationMethod method, String input) {
            this.docId = docId;
            this.method = method;
            this.input = input;
        }

        public DocIdDerivation toProto() {
            return DocIdDerivation.newBuilder()
                    .setMethod(method)
                    .setInput(input)
                    .setCanonicalInput(docId.substring(docId.indexOf(':') + 1))
                    .build();
        }
    }
}
