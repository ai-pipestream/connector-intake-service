package ai.pipeline.connector.intake.queue;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

/**
 * Test profile that strips intake's full Quarkus boot down to just what
 * the Redis-backed queue tests need: the dev-services Redis container
 * and CDI. Disables Postgres / Hibernate / Flyway / Kafka / Apicurio /
 * Consul registration so {@code @QuarkusTest} cold-start drops from
 * tens of seconds to single-digit.
 *
 * <p>Also disables the production {@link IntakeJobWorkerPool} so it
 * doesn't compete with the test's hand-built workers for queue items.
 */
public class IntakeQueueTestProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        // Note on what's NOT disabled: Postgres dev service stays on — the
        // intake's @GrpcService directly injects a Hibernate SessionFactory
        // (via SessionManager) and refuses to deploy when Hibernate is
        // inactive. The Postgres container is shared across tests by
        // default, so cold-start cost is paid once per JVM, not per test.
        return Map.ofEntries(
                // Kafka — incoming/outgoing channels belong to the intake
                // flow, not the queue. Avoid the multi-second Kafka
                // container startup.
                Map.entry("quarkus.kafka.devservices.enabled", "false"),

                // Apicurio Registry — only needed once Kafka serdes are
                // wired. Disable to avoid its container startup.
                Map.entry("quarkus.apicurio-registry.devservices.enabled", "false"),

                // Skip Consul self-registration; LocalServiceIdentity still
                // resolves from config (service name + advertised host/port).
                Map.entry("pipestream.registration.enabled", "false"),
                Map.entry("pipestream.registration.re-registration.enabled", "false"),

                // Don't auto-start the production worker pool — the tests
                // construct their own IntakeJobWorker instances.
                Map.entry("pipestream.intake.queue.enabled", "false")
        );
    }

    @Override
    public String getConfigProfile() {
        return "queue-test";
    }
}
