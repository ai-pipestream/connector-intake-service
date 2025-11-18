package ai.pipeline.connector.intake;

import com.github.tomakehurst.wiremock.client.WireMock;
import org.wiremock.grpc.dsl.WireMockGrpcService;
import ai.pipestream.repository.filesystem.upload.InitiateUploadRequest;
import ai.pipestream.repository.filesystem.upload.InitiateUploadResponse;
import ai.pipestream.repository.filesystem.upload.UploadState;

import static ai.pipestream.grpc.wiremock.WireMockGrpcCompat.*;

/**
 * Ready-to-use mock utilities for the Repository Service (NodeUploadService).
 * Uses standard gRPC mocking that works with both standard and Mutiny clients.
 */
public class RepositoryServiceMock {

    private final WireMockGrpcService mockService;

    /**
     * Creates a repository service mock for the given WireMock port.
     *
     * @param wireMockPort The port where WireMock is running
     */
    public RepositoryServiceMock(int wireMockPort) {
        this.mockService = new WireMockGrpcService(
            new WireMock(wireMockPort), 
            "ai.pipestream.repository.filesystem.upload.NodeUploadService"
        );
    }

    /**
     * Mock successful upload initiation.
     *
     * @param nodeId The node ID to return
     * @param uploadId The upload ID to return
     * @return this instance for method chaining
     */
    public RepositoryServiceMock mockInitiateUpload(String nodeId, String uploadId) {
        mockService.stubFor(
            method("InitiateUpload")
                .willReturn(message(
                    InitiateUploadResponse.newBuilder()
                        .setNodeId(nodeId)
                        .setUploadId(uploadId)
                        .setState(UploadState.UPLOAD_STATE_PENDING)
                        .setCreatedAtEpochMs(System.currentTimeMillis())
                        .setIsUpdate(false)
                        .build()
                ))
        );
        
        return this;
    }

    /**
     * Mock successful upload initiation with specific request matching.
     *
     * @param nodeId The node ID to return
     * @param uploadId The upload ID to return
     * @param expectedRequest The expected request to match
     * @return this instance for method chaining
     */
    public RepositoryServiceMock mockInitiateUpload(String nodeId, String uploadId, InitiateUploadRequest expectedRequest) {
        mockService.stubFor(
            method("InitiateUpload")
                .withRequestMessage(equalToMessage(expectedRequest))
                .willReturn(message(
                    InitiateUploadResponse.newBuilder()
                        .setNodeId(nodeId)
                        .setUploadId(uploadId)
                        .setState(UploadState.UPLOAD_STATE_PENDING)
                        .setCreatedAtEpochMs(System.currentTimeMillis())
                        .setIsUpdate(false)
                        .build()
                ))
        );
        
        return this;
    }

    /**
     * Mock upload initiation with default values.
     * Creates a simple successful response with generated IDs.
     *
     * @return this instance for method chaining
     */
    public RepositoryServiceMock mockInitiateUpload() {
        return mockInitiateUpload(
            "test-node-id-" + System.currentTimeMillis(),
            "test-upload-id-" + System.currentTimeMillis()
        );
    }

    /**
     * Get the underlying WireMockGrpcService for advanced usage.
     *
     * @return the WireMockGrpcService instance
     */
    public WireMockGrpcService getService() {
        return mockService;
    }
}

