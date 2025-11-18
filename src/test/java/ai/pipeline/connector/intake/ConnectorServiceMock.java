package ai.pipeline.connector.intake;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.protobuf.Timestamp;
import org.wiremock.grpc.dsl.WireMockGrpcService;
import ai.pipestream.connector.intake.ConnectorRegistration;
import ai.pipestream.connector.intake.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.ValidateApiKeyResponse;

import static ai.pipestream.grpc.wiremock.WireMockGrpcCompat.*;

/**
 * Ready-to-use mock utilities for the Connector Service (ConnectorAdminService).
 * Uses standard gRPC mocking that works with both standard and Mutiny clients.
 */
public class ConnectorServiceMock {

    private final WireMockGrpcService mockService;

    /**
     * Creates a connector service mock for the given WireMock port.
     *
     * @param wireMockPort The port where WireMock is running
     */
    public ConnectorServiceMock(int wireMockPort) {
        this.mockService = new WireMockGrpcService(
            new WireMock(wireMockPort), 
            "ai.pipestream.connector.intake.ConnectorAdminService"
        );
    }

    /**
     * Mock successful API key validation.
     *
     * @param connectorId The connector ID
     * @param apiKey The API key to validate
     * @param accountId The account ID that owns the connector
     * @return this instance for method chaining
     */
    public ConnectorServiceMock mockValidateApiKey(String connectorId, String apiKey, String accountId) {
        mockService.stubFor(
            method("ValidateApiKey")
                .withRequestMessage(equalToMessage(
                    ValidateApiKeyRequest.newBuilder()
                        .setConnectorId(connectorId)
                        .setApiKey(apiKey)
                        .build()
                ))
                .willReturn(message(
                    ValidateApiKeyResponse.newBuilder()
                        .setValid(true)
                        .setMessage("API key validated successfully")
                        .setConnector(ConnectorRegistration.newBuilder()
                            .setConnectorId(connectorId)
                            .setConnectorName("Test Connector")
                            .setConnectorType("filesystem")
                            .setAccountId(accountId)
                            .setApiKey(apiKey)
                            .setS3Bucket("test-bucket")
                            .setS3BasePath("test/path/")
                            .setMaxFileSize(1024 * 1024 * 1024) // 1GB
                            .setRateLimitPerMinute(1000)
                            .setActive(true)
                            .setCreated(Timestamp.newBuilder()
                                .setSeconds(System.currentTimeMillis() / 1000)
                                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                                .build())
                            .setUpdated(Timestamp.newBuilder()
                                .setSeconds(System.currentTimeMillis() / 1000)
                                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                                .build())
                            .build())
                        .build()
                ))
        );
        
        return this;
    }

    /**
     * Mock successful API key validation with custom connector registration.
     *
     * @param connectorId The connector ID
     * @param apiKey The API key to validate
     * @param connector The connector registration to return
     * @return this instance for method chaining
     */
    public ConnectorServiceMock mockValidateApiKey(String connectorId, String apiKey, ConnectorRegistration connector) {
        mockService.stubFor(
            method("ValidateApiKey")
                .withRequestMessage(equalToMessage(
                    ValidateApiKeyRequest.newBuilder()
                        .setConnectorId(connectorId)
                        .setApiKey(apiKey)
                        .build()
                ))
                .willReturn(message(
                    ValidateApiKeyResponse.newBuilder()
                        .setValid(true)
                        .setMessage("API key validated successfully")
                        .setConnector(connector)
                        .build()
                ))
        );
        
        return this;
    }

    /**
     * Mock failed API key validation.
     *
     * @param connectorId The connector ID
     * @param apiKey The invalid API key
     * @param errorMessage The error message to return
     * @return this instance for method chaining
     */
    public ConnectorServiceMock mockValidateApiKeyFailed(String connectorId, String apiKey, String errorMessage) {
        mockService.stubFor(
            method("ValidateApiKey")
                .withRequestMessage(equalToMessage(
                    ValidateApiKeyRequest.newBuilder()
                        .setConnectorId(connectorId)
                        .setApiKey(apiKey)
                        .build()
                ))
                .willReturn(message(
                    ValidateApiKeyResponse.newBuilder()
                        .setValid(false)
                        .setMessage(errorMessage)
                        .build()
                ))
        );
        
        return this;
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

