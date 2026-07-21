package za.co.fnb.dcre.crr.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.crr.data.model.TxHeaderEntity;
import za.co.fnb.dcre.crr.data.repo.TxHeaderRepo;
import za.co.fnb.dcre.platform.files.Layouts;
import za.co.fnb.dcre.platform.files.R31Filename;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Business tier (configuration.md point 21): header parse + the file-fatal
 * structural tier (R-19): length, version incl. V1 fail-closed (A-2),
 * declared count, R-31 filename-vs-header cross-check. Persists via the
 * data/repo tier only.
 */
@Service
public class HeaderService {

    /** Default flow: DC collections. AGT passes flow=PAY for ENDO arrivals (SCRUM-69). */
    static final String FLOW_COL = "COL";

    private final TxHeaderRepo repo;
    private final boolean v1Enabled;

    public HeaderService(TxHeaderRepo repo, @Value("${dcre.v1-enabled}") boolean v1Enabled) {
        this.repo = repo;
        this.v1Enabled = v1Enabled;
    }

    /** @return the file-fatal reason, or empty when the header was accepted and persisted. */
    public Optional<String> ingestHeader(UUID arrivalId, Path input, String originalName, String flow)
            throws IOException {
        try {
            // ISO_8859_1: byte-transparent (one byte = one char), same contract as
            // the partitioned range reader; strict UTF-8 would crash on legacy bytes
            List<String> lines = Files.readAllLines(input, java.nio.charset.StandardCharsets.ISO_8859_1);
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
            Optional<R31Filename.Tokens> tokens = originalName != null
                    ? R31Filename.parse(originalName) : Optional.empty();
            if (tokens.isPresent() && !tokens.get().client().equals(destination)) {
                throw new FileFatalException("R-31 mismatch: filename client " + tokens.get().client()
                        + " != header destination " + destination);
            }
            OpaqueRef msgId = OpaqueRef.ofFixedWidth(header.substring(4, 26));
            repo.upsert(TxHeaderEntity.of(arrivalId, msgId.rawBytes(), msgId.canonical(),
                    Layouts.HEADER.slice(header, "created_ts"), declared, destination,
                    Layouts.HEADER.slice(header, "business_date"),
                    tokens.map(R31Filename.Tokens::client).orElse(null), version,
                    flow == null || flow.isBlank() ? FLOW_COL : flow));
            return Optional.empty();
        } catch (FileFatalException e) {
            return Optional.of(e.getMessage());
        }
    }
}
