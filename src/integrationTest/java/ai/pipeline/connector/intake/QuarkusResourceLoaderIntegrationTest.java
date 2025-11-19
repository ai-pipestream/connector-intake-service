package ai.pipeline.connector.intake;

import ai.pipestream.data.util.QuarkusResourceLoader;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify QuarkusResourceLoader works when used as a dependency
 * in connector-intake-service. This tests the new loadTestDocument and listResourcePaths methods.
 * 
 * This test runs against the built JAR artifact using @QuarkusIntegrationTest.
 * No CDI injection is available - we use static methods directly.
 */
@QuarkusIntegrationTest
@DisplayName("QuarkusResourceLoader Integration Test")
class QuarkusResourceLoaderIntegrationTest {

    @Test
    @DisplayName("Should load test document from test-documents JAR")
    void testLoadTestDocument() throws IOException {
        byte[] content = QuarkusResourceLoader.loadTestDocument("sample_image/sample.jpg");
        assertNotNull(content, "Document content should not be null");
        assertTrue(content.length > 0, "Document should have content");
    }

    @Test
    @DisplayName("Should list resource paths from test-documents JAR")
    void testListResourcePaths() {
        List<String> paths = QuarkusResourceLoader.listResourcePaths("sample_image");
        assertNotNull(paths, "Paths list should not be null");
        assertFalse(paths.isEmpty(), "Should find at least one file in sample_image");
        
        // Verify we can load a document from the listed path
        String firstPath = paths.get(0);
        assertDoesNotThrow(() -> {
            byte[] content = QuarkusResourceLoader.loadTestDocument(firstPath);
            assertNotNull(content);
            assertTrue(content.length > 0);
        }, "Should be able to load document from listed path");
    }

    @Test
    @DisplayName("Should load multiple document types")
    void testLoadMultipleDocumentTypes() throws IOException {
        // Test PDF
        byte[] pdf = QuarkusResourceLoader.loadTestDocument("sample_miscellaneous_files/sample.pdf");
        assertNotNull(pdf);
        assertTrue(pdf.length > 0);
        
        // Test text
        byte[] text = QuarkusResourceLoader.loadTestDocument("sample_text/sample.txt");
        assertNotNull(text);
        assertTrue(text.length > 0);
    }
}

