package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.data.v1.DocIdDerivation;
import ai.pipestream.data.v1.DocIdDerivationMethod;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipeDocHandoffDraftTest {

    @Test
    void buildForHandoffAppliesDerivedFieldsAndOwnershipAtOutputTime() {
        PipeDoc original = PipeDoc.newBuilder()
                .setDocId("")
                .setSearchMetadata(SearchMetadata.newBuilder().setSourcePath("/source/path").build())
                .build();
        DocIdDerivation derivation = DocIdDerivation.newBuilder()
                .setMethod(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_DOC_ID)
                .setInput("source-1")
                .setCanonicalInput("source-1")
                .build();
        ConfigResolutionService.ResolvedConfig resolved = resolvedConfig("account-1", "datasource-1");

        PipeDocHandoffDraft draft = new PipeDocHandoffDraft(original, "datasource-1:source-1", derivation);

        PipeDoc built = draft.buildForHandoff(resolved);

        assertEquals("", original.getDocId(), "the original request message is not rewritten early");
        assertFalse(original.hasOwnership(), "ownership is not stamped onto the original request message");
        assertEquals("datasource-1:source-1", built.getDocId());
        assertEquals(derivation, built.getDocIdDerivation());
        assertEquals("account-1", built.getOwnership().getAccountId());
        assertEquals("datasource-1", built.getOwnership().getDatasourceId());
        assertEquals(original.getSearchMetadata(), built.getSearchMetadata());
    }

    @Test
    void buildForHandoffPreservesExistingOwnership() {
        OwnershipContext ownership = OwnershipContext.newBuilder()
                .setAccountId("caller-account")
                .setDatasourceId("caller-datasource")
                .build();
        PipeDoc original = PipeDoc.newBuilder()
                .setDocId("client-doc")
                .setOwnership(ownership)
                .build();
        DocIdDerivation derivation = DocIdDerivation.newBuilder()
                .setMethod(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CLIENT_PROVIDED)
                .setInput("client-doc")
                .setCanonicalInput("client-doc")
                .build();

        PipeDoc built = new PipeDocHandoffDraft(original, "client-doc", derivation)
                .buildForHandoff(resolvedConfig("resolved-account", "resolved-datasource"));

        assertEquals(ownership, built.getOwnership());
    }

    private static ConfigResolutionService.ResolvedConfig resolvedConfig(String accountId, String datasourceId) {
        return new ConfigResolutionService.ResolvedConfig(
                DataSourceConfig.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId)
                        .build(),
                IngestionConfig.getDefaultInstance());
    }
}
