package za.co.fnb.dcre.crr.service;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.platform.files.Layouts;
import za.co.fnb.dcre.platform.files.R31Filename;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * Parses and persists the header; runs the file-fatal structural tier
 * (R-19): header length, layout version, filename-vs-header cross-check
 * (R-31), declared-count sanity. A structural failure ends the JOB
 * COMPLETED with exit status FILE_FATAL: a business verdict for CIR,
 * never a process death (R-33).
 */
@Component
public class HeaderTasklet implements Tasklet {

    public static final String EXIT_FILE_FATAL = "FILE_FATAL";

    private final JdbcTemplate jdbc;
    private final boolean v1Enabled;

    public HeaderTasklet(JdbcTemplate jdbc, @Value("${dcre.v1-enabled}") boolean v1Enabled) {
        this.jdbc = jdbc;
        this.v1Enabled = v1Enabled;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        UUID arrivalId = UUID.fromString((String) params.get("arrival.id"));
        Path input = Path.of((String) params.get("input.file"));
        String originalName = (String) params.get("original.name");

        try {
            List<String> lines = Files.readAllLines(input);
            if (lines.isEmpty()) {
                throw new FileFatalException("empty file");
            }
            String header = lines.get(0);
            if (header.length() < Layouts.HEADER.length()) {
                throw new FileFatalException("header shorter than attested content length 109");
            }
            int version = Integer.parseInt(Layouts.HEADER.slice(header, "layout_version").strip());
            if (version == 1 && !v1Enabled) {
                throw new FileFatalException("V1 layout fails closed in production (A-2)");
            }
            int declared = Integer.parseInt(Layouts.HEADER.slice(header, "tx_count").strip());
            int actual = lines.size() - 1;
            if (declared != actual) {
                throw new FileFatalException("header tx_count=" + declared + " but file has " + actual);
            }
            String destination = Layouts.HEADER.slice(header, "destination_id").strip();
            var tokens = originalName != null ? R31Filename.parse(originalName) : java.util.Optional.<R31Filename.Tokens>empty();
            if (tokens.isPresent() && !tokens.get().client().equals(destination)) {
                throw new FileFatalException("R-31 mismatch: filename client " + tokens.get().client()
                        + " != header destination " + destination);
            }
            OpaqueRef msgId = OpaqueRef.ofFixedWidth(header.substring(4, 26)); // sender+type+ts+version composite
            jdbc.update("""
                    UPSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,
                        initg_pty, business_date, client_token, layout_version)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    arrivalId, msgId.rawBytes(), msgId.canonical(),
                    Layouts.HEADER.slice(header, "created_ts"),
                    declared, destination,
                    Layouts.HEADER.slice(header, "business_date"),
                    tokens.map(R31Filename.Tokens::client).orElse(null),
                    version);
        } catch (FileFatalException e) {
            chunkContext.getStepContext().getStepExecution().getJobExecution()
                    .getExecutionContext().putString("fileFatalReason", e.getMessage());
            contribution.setExitStatus(new ExitStatus(EXIT_FILE_FATAL, e.getMessage()));
        }
        return RepeatStatus.FINISHED;
    }
}
