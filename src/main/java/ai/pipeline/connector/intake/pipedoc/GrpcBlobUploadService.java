package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;

@ApplicationScoped
public class GrpcBlobUploadService {

    private static final Logger LOG = Logger.getLogger(GrpcBlobUploadService.class);

    @Inject
    ConfigResolutionService configResolutionService;

    @Inject
    PipeDocIdDeriver pipeDocIdDeriver;

    @Inject
    RepositoryPipeDocHandoffService repositoryPipeDocHandoffService;

    public void uploadBlob(UploadBlobRequest request,
                           StreamObserver<UploadBlobResponse> responseObserver) {
        try {
            responseObserver.onNext(handleUploadBlob(request));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    UploadBlobResponse handleUploadBlob(UploadBlobRequest request) {
        if (request.getDatasourceId().isBlank()) {
            return UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: datasource_id is required")
                    .build();
        }
        if (request.getApiKey().isBlank()) {
            return UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: api_key is required")
                    .build();
        }

        long startTime = System.nanoTime();
        int requestSize = request.getSerializedSize();
        LOG.debugf("uploadBlob START: size=%d bytes", requestSize);

        try {
            ConfigResolutionService.ResolvedConfig resolved =
                    configResolutionService.resolveConfig(request.getDatasourceId(), request.getApiKey());
            long configTime = System.nanoTime() - startTime;
            LOG.debugf("uploadBlob: config resolution took %.3f ms", configTime / 1_000_000.0);

            Instant now = Instant.now();
            Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

            Blob blob = Blob.newBuilder()
                    .setData(request.getContent())
                    .setFilename(request.getFilename())
                    .setMimeType(request.getMimeType())
                    .build();

            SearchMetadata metadata = SearchMetadata.newBuilder()
                    .setCreationDate(timestamp)
                    .setLastModifiedDate(timestamp)
                    .setProcessedDate(timestamp)
                    .setSourcePath(request.getPath())
                    .putAllMetadata(request.getMetadataMap())
                    .putMetadata("datasource_id", request.getDatasourceId())
                    .putMetadata("account_id", resolved.tier1Config().getAccountId())
                    .build();

            PipeDocIdDeriver.DocIdDerivationResult derivation = pipeDocIdDeriver.derive(
                    request.getDatasourceId(), null, request.getSourceDocId(), metadata);
            if (derivation == null) {
                return UploadBlobResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Validation error: Cannot determine doc_id deterministically for blob upload. "
                                + "Provide source_doc_id or set source_path in metadata")
                        .build();
            }

            LOG.debugf("uploadBlob: derived doc_id=%s via %s", derivation.docId, derivation.method);
            OwnershipContext ownership = OwnershipContext.newBuilder()
                    .setAccountId(resolved.tier1Config().getAccountId())
                    .setConnectorId(resolved.tier1Config().getConnectorId())
                    .setDatasourceId(request.getDatasourceId())
                    .build();

            PipeDoc pipeDoc = PipeDoc.newBuilder()
                    .setDocId(derivation.docId)
                    .setDocIdDerivation(derivation.toProto())
                    .setOwnership(ownership)
                    .setSearchMetadata(metadata)
                    .setBlobBag(BlobBag.newBuilder().setBlob(blob).build())
                    .build();

            return repositoryPipeDocHandoffService.persistBlobAndHandoff(
                    pipeDoc, resolved, startTime, request.getCrawlId());
        } catch (RuntimeException e) {
            long totalTime = System.nanoTime() - startTime;
            LOG.errorf(e, "Failed to upload Blob after %.3f ms", totalTime / 1_000_000.0);
            return UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId("")
                    .setMessage(e.getMessage())
                    .build();
        }
    }
}
