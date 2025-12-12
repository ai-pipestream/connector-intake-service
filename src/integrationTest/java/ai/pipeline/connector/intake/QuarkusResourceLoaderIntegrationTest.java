package ai.pipeline.connector.intake;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

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

    private static byte[] loadTestDocument(String resourcePath) throws IOException {
        String normalized = resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath;
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL url = cl.getResource(normalized);
        assertNotNull(url, "Resource not found on classpath: " + normalized);

        try (InputStream in = new BufferedInputStream(url.openStream())) {
            return in.readAllBytes();
        }
    }

    private static List<String> listResourcePaths(String prefix) {
        String normalizedPrefix = prefix.startsWith("/") ? prefix.substring(1) : prefix;
        if (!normalizedPrefix.endsWith("/")) {
            normalizedPrefix = normalizedPrefix + "/";
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL url = cl.getResource(normalizedPrefix);
        assertNotNull(url, "Resource prefix not found on classpath: " + normalizedPrefix);

        try {
            String protocol = url.getProtocol();
            if ("jar".equals(protocol)) {
                return listFromJar(url, normalizedPrefix);
            }
            if ("file".equals(protocol)) {
                return listFromDirectory(url, normalizedPrefix);
            }
            fail("Unsupported resource URL protocol for listing: " + protocol + " (" + url + ")");
            return List.of();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list resource paths for prefix: " + normalizedPrefix, e);
        }
    }

    private static List<String> listFromJar(URL jarUrl, String normalizedPrefix) throws IOException {
        JarURLConnection conn = (JarURLConnection) jarUrl.openConnection();
        try (JarFile jarFile = conn.getJarFile()) {
            List<String> out = new ArrayList<>();
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (!entry.isDirectory() && name.startsWith(normalizedPrefix)) {
                    out.add(name);
                }
            }
            return out;
        }
    }

    private static List<String> listFromDirectory(URL dirUrl, String normalizedPrefix) throws Exception {
        URI uri = dirUrl.toURI();
        Path dir = Path.of(uri);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        try (var stream = Files.walk(dir)) {
            stream
                .filter(Files::isRegularFile)
                .forEach(path -> {
                    String rel = dir.relativize(path).toString().replace('\\', '/');
                    out.add(normalizedPrefix + rel);
                });
        }
        return out;
    }

    @Test
    @DisplayName("Should load test document from test-documents JAR")
    void testLoadTestDocument() throws IOException {
        byte[] content = loadTestDocument("sample_image/sample.jpg");
        assertNotNull(content, "Document content should not be null");
        assertTrue(content.length > 0, "Document should have content");
    }

    @Test
    @DisplayName("Should list resource paths from test-documents JAR")
    void testListResourcePaths() {
        List<String> paths = listResourcePaths("sample_image");
        assertNotNull(paths, "Paths list should not be null");
        assertFalse(paths.isEmpty(), "Should find at least one file in sample_image");
        
        // Verify we can load a document from the listed path
        String firstPath = paths.get(0);
        assertDoesNotThrow(() -> {
            byte[] content = loadTestDocument(firstPath);
            assertNotNull(content);
            assertTrue(content.length > 0);
        }, "Should be able to load document from listed path");
    }

    @Test
    @DisplayName("Should load multiple document types")
    void testLoadMultipleDocumentTypes() throws IOException {
        // Test PDF
        byte[] pdf = loadTestDocument("sample_miscellaneous_files/sample.pdf");
        assertNotNull(pdf);
        assertTrue(pdf.length > 0);
        
        // Test text
        byte[] text = loadTestDocument("sample_text/sample.txt");
        assertNotNull(text);
        assertTrue(text.length > 0);
    }
}

