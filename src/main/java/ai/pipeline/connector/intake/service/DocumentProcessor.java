package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.UploadBlobRequest;
import ai.pipestream.connector.intake.UploadPipeDocRequest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Simplified Document Processor.
 * Logic for enriching and wrapping documents before sending to Repo.
 * DB tracking and SHA calculation moved to async Kafka handlers (post-upload).
 */
@ApplicationScoped
public class DocumentProcessor {

    private static final Logger LOG = Logger.getLogger(DocumentProcessor.class);

    /**
     * Default constructor for CDI.
     */
    public DocumentProcessor() { }

    /**
     * Process a PipeDoc upload request.
     * Currently a pass-through, but placeholder for future pre-upload validation/enrichment.
     *
     * @param request the inbound PipeDoc request to process
     * @return a {@code Uni<Void>} that completes when processing finishes
     */
    public Uni<Void> processPipeDoc(UploadPipeDocRequest request) {
        // No-op for now; validation happens in ConnectorIntakeServiceImpl
        return Uni.createFrom().voidItem();
    }

    /**
     * Process a Blob upload request.
     * Currently a pass-through, but placeholder for future pre-upload validation/enrichment.
     *
     * @param request the inbound Blob request to process
     * @return a {@code Uni<Void>} that completes when processing finishes
     */
    public Uni<Void> processBlob(UploadBlobRequest request) {
        // No-op for now; validation happens in ConnectorIntakeServiceImpl
        return Uni.createFrom().voidItem();
    }
}
