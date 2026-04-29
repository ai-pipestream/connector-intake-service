package ai.pipeline.connector.intake.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectorMetadataTest {

    @Test
    void fromJsonRejectsMalformedMetadataInsteadOfDefaulting() {
        assertThrows(IllegalArgumentException.class, () -> ConnectorMetadata.fromJson("{not-json"));
    }

    @Test
    void fromJsonRejectsWrongTypedMetadataInsteadOfDefaulting() {
        assertThrows(IllegalArgumentException.class, () -> ConnectorMetadata.fromJson("{\"maxFileSize\":\"large\"}"));
    }
}
