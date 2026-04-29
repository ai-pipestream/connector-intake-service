package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.data.v1.PipeDoc;

@FunctionalInterface
public interface PersistedPipeDocHandler {
    UploadPipeDocResponse persistAndHandoff(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            long startTime,
            String crawlId);
}
