package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Simplified Document Processor.
 */
@ApplicationScoped
public class DocumentProcessor {

    public DocumentProcessor() { }

    public void processPipeDoc(UploadPipeDocRequest request) {
        // No-op for now; validation happens in the upload service.
    }

    public void processBlob(UploadBlobRequest request) {
        // No-op for now; validation happens in the upload service.
    }
}
