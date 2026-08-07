package za.co.fnb.dcre.crr.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.crr.data.model.TxHeaderEntity;
import za.co.fnb.dcre.crr.data.repo.TxHeaderRepo;
import za.co.fnb.dcre.platform.copybook.CopybookReader;
import za.co.fnb.dcre.platform.copybook.FixedWidthRecord;
import za.co.fnb.dcre.platform.copybook.ShortRecordException;
import za.co.fnb.dcre.platform.files.R31Filename;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Business tier (configuration.md point 21): header parse + the file-fatal
 * structural tier (R-19): length, version incl. V1 fail-closed (A-2),
 * declared count, R-31 filename-vs-header cross-check. Persists via the
 * data/repo tier only.
 *
 * <p>The copybook read itself (ISO_8859_1 byte-transparent decode, fail-closed
 * record-length gate) lives in platform-copybook, shared with the payments and
 * mandates readers so a fix reaches all three instead of one fork.
 */
@Service
public class HeaderService {

    /** Default flow: DC collections. AGT passes flow=PAY for ENDO arrivals (SCRUM-69). */
    static final String FLOW_COL = "COL";
    static final String FLOW_PAY = "PAY";

    private final TxHeaderRepo repo;
    private final boolean v1Enabled;

    public HeaderService(TxHeaderRepo repo, @Value("${dcre.v1-enabled}") boolean v1Enabled) {
        this.repo = repo;
        this.v1Enabled = v1Enabled;
    }

    /** @return the file-fatal reason, or empty when the header was accepted and persisted. */
    public Optional<String> ingestHeader(UUID arrivalId, Path input, String originalName, String flow)
            throws IOException {
        final String stampedFlow = validatedFlow(flow);
        try {
            List<FixedWidthRecord> records = readRecords(input);
            if (records.isEmpty()) {
                throw new FileFatalException("empty file");
            }
            FixedWidthRecord headerRecord = records.get(0);
            String header = headerRecord.line();
            int version = Integer.parseInt(headerRecord.field("layout_version").strip());
            if (version == 1 && !v1Enabled) {
                throw new FileFatalException("V1 layout fails closed in production (A-2)");
            }
            int declared = Integer.parseInt(headerRecord.field("tx_count").strip());
            int actual = records.size() - 1;
            if (declared != actual) {
                throw new FileFatalException("header tx_count=" + declared + " but file has " + actual);
            }
            String destination = headerRecord.field("destination_id").strip();
            Optional<R31Filename.Tokens> tokens = originalName != null
                    ? R31Filename.parse(originalName) : Optional.empty();
            if (tokens.isPresent() && !tokens.get().client().equals(destination)) {
                throw new FileFatalException("R-31 mismatch: filename client " + tokens.get().client()
                        + " != header destination " + destination);
            }
            OpaqueRef msgId = OpaqueRef.ofFixedWidth(header.substring(4, 26));
            repo.upsert(TxHeaderEntity.of(arrivalId, msgId.rawBytes(), msgId.canonical(),
                    headerRecord.field("created_ts"), declared, destination,
                    headerRecord.field("business_date"),
                    tokens.map(R31Filename.Tokens::client).orElse(null), version, stampedFlow));
            return Optional.empty();
        } catch (FileFatalException e) {
            return Optional.of(e.getMessage());
        }
    }

    /** The shared copybook read, resolving each record's layout through
     *  {@link CollectionRecords} so the header is cut by the header table and a
     *  detail by its own. The short-record failure is translated into this
     *  service's file-fatal vocabulary (R-19): record 0 short is the
     *  malformed-header case the boundary reader has always reported; a later
     *  short record is a ragged body, reported as itself, not blamed on the header. */
    private static List<FixedWidthRecord> readRecords(Path input) throws IOException {
        try {
            return CopybookReader.read(input, CollectionRecords.INSTANCE);
        } catch (ShortRecordException e) {
            throw new FileFatalException(e.recordIndex() == 0
                    ? "header shorter than attested content length " + e.declaredLength()
                    : e.getMessage());
        }
    }

    /**
     * Closed flow vocabulary (review m1, fail closed): COL or PAY only,
     * absent/blank defaults to COL. Any other value is a launcher
     * misconfiguration, never a business verdict about the FILE, so it
     * throws (job FAILED) instead of returning a file-fatal reason.
     */
    private static String validatedFlow(String flow) {
        if (flow == null || flow.isBlank()) {
            return FLOW_COL;
        }
        if (!FLOW_COL.equals(flow) && !FLOW_PAY.equals(flow)) {
            throw new IllegalArgumentException(
                    "unknown flow launch parameter '" + flow + "': COL or PAY only (fail closed)");
        }
        return flow;
    }
}
