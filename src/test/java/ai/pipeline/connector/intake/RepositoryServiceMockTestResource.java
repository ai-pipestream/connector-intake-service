package ai.pipeline.connector.intake;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import ai.pipestream.grpc.wiremock.InjectWireMock;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.util.Map;

/**
 * Quarkus test resource for mocking repository-service.
 * <p>
 * Starts WireMock with gRPC extension and configures Stork static discovery
 * to route repository-service calls to WireMock instead of Consul.
 * <p>
 * Usage:
 * <pre>
 * {@code
 * @QuarkusTest
 * @QuarkusTestResource(RepositoryServiceMockTestResource.class)
 * public class MyTest {
 *     @InjectWireMock
 *     WireMockServer wireMockServer;
 *
 *     @BeforeEach
 *     void setup() {
 *         new RepositoryServiceMock(wireMockServer.port())
 *             .mockInitiateUpload("test-node-id", "test-upload-id");
 *     }
 * }
 * }
 * </pre>
 */
public class RepositoryServiceMockTestResource implements QuarkusTestResourceLifecycleManager {

    /**
     * The WireMock server instance.
     */
    private WireMockServer wireMockServer;

    /**
     * Default constructor.
     */
    public RepositoryServiceMockTestResource() {
    }

    @Override
    public Map<String, String> start() {
        // Start WireMock with gRPC extension
        wireMockServer = new WireMockServer(
            WireMockConfiguration.wireMockConfig()
                .dynamicPort()
                .usingFilesUnderClasspath("META-INF")
                .extensions(new GrpcExtensionFactory())
        );
        wireMockServer.start();

        int mockPort = wireMockServer.port();

        // Return Stork configuration to route services to WireMock
        return Map.ofEntries(
            // Stork static discovery for repository-service
            Map.entry("quarkus.stork.repository-service.service-discovery.type", "static"),
            Map.entry("quarkus.stork.repository-service.service-discovery.address-list", "localhost:" + mockPort),

            // Stork static discovery for connector-service (needed for connector validation)
            Map.entry("quarkus.stork.connector-service.service-discovery.type", "static"),
            Map.entry("quarkus.stork.connector-service.service-discovery.address-list", "localhost:" + mockPort),

            // Stork static discovery for account-manager (needed for account validation)
            Map.entry("quarkus.stork.account-manager.service-discovery.type", "static"),
            Map.entry("quarkus.stork.account-manager.service-discovery.address-list", "localhost:" + mockPort),

            // System property for injecting WireMock server
            Map.entry("test.wiremock.port", String.valueOf(mockPort)),

            // Disable service registration in tests
            Map.entry("service.registration.enabled", "false")
        );
    }

    /**
     * {@inheritDoc}
     * 
     * <p>Stops the WireMock server.
     */
    @Override
    public void stop() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * <p>Injects the WireMock server into test fields annotated with @InjectWireMock.
     */
    @Override
    public void inject(TestInjector testInjector) {
        // Inject WireMock server into test fields annotated with @InjectWireMock
        testInjector.injectIntoFields(
            wireMockServer,
            new TestInjector.AnnotatedAndMatchesType(InjectWireMock.class, WireMockServer.class)
        );
    }
}

