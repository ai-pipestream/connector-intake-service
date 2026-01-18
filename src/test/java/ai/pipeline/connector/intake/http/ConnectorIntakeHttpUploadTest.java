package ai.pipeline.connector.intake.http;

import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
@QuarkusTestResource(RepositoryUploadTestResource.class)
class ConnectorIntakeHttpUploadTest {

    @BeforeEach
    void reset() {
        RepositoryUploadMockState.reset();
    }

    @Test
    void uploadRaw_proxiesToRepositoryService() {
        byte[] payload = "hello-world".getBytes(StandardCharsets.UTF_8);

        given()
            .header("x-datasource-id", "valid-datasource")
            .header("x-api-key", "valid-api-key")
            .header("x-source-uri", "s3://example-bucket/path/to/object.txt?versionId=1")
            .header("x-filename", "object.txt")
            .header("x-checksum-sha256", "abc123")
            .contentType("text/plain")
            .body(payload)
        .when()
            .post("/uploads/raw")
        .then()
            .statusCode(200)
            .contentType("application/json");

        assertEquals("/internal/uploads/raw", RepositoryUploadMockState.lastPath());

        Map<String, String> headers = RepositoryUploadMockState.lastHeaders();
        assertEquals("valid-account", headers.get("x-account-id"));
        assertTrue(headers.containsKey("x-connector-id"));
        assertTrue(headers.containsKey("x-drive-name"));
        assertEquals("object.txt", headers.get("x-filename"));
        assertEquals("abc123", headers.get("x-checksum-sha256"));
        assertEquals(String.valueOf(payload.length), headers.get("content-length"));

        String docId = headers.get("x-doc-id");
        assertNotNull(docId);
        assertTrue(docId.startsWith("valid-datasource:s3://example-bucket/path/to/object.txt"));

        assertArrayEquals(payload, RepositoryUploadMockState.lastBody());
    }
}
