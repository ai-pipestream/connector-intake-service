package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.data.v1.DocIdDerivation;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;

/**
 * Request-local, immutable data needed to assemble the final PipeDoc at handoff.
 */
record PipeDocHandoffDraft(
        PipeDoc originalPipeDoc,
        String docId,
        DocIdDerivation docIdDerivation) {

    PipeDoc buildForHandoff(ConfigResolutionService.ResolvedConfig resolved) {
        PipeDoc.Builder builder = originalPipeDoc.toBuilder()
                .setDocId(docId)
                .setDocIdDerivation(docIdDerivation);

        if (!originalPipeDoc.hasOwnership()) {
            builder.setOwnership(OwnershipContext.newBuilder()
                    .setAccountId(resolved.tier1Config().getAccountId())
                    .setDatasourceId(resolved.tier1Config().getDatasourceId())
                    .build());
        }

        return builder.build();
    }
}
