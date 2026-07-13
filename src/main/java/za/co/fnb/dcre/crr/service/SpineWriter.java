package za.co.fnb.dcre.crr.service;

import za.co.fnb.dcre.crr.data.model.TxEntryEntity;
import za.co.fnb.dcre.crr.data.repo.TxEntryBatchDao;
import za.co.fnb.dcre.platform.files.FixedWidthLayout;
import za.co.fnb.dcre.platform.files.Layouts;
import za.co.fnb.dcre.platform.model.MoneyText;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Persists detail lines via the guarded UPSERT keyed (arrival_id, sequence):
 * a restarted chunk rewrites identical rows, never duplicates (R-05); writes
 * are batched per chunk (R-41). Each row carries a SHA-256 content hash over
 * the essential business fields for CTV's in-file dup scan (R-41). V1 (161)
 * fails closed unless dcre.v1-enabled (A-2).
 */
public class SpineWriter {

    private final TxEntryBatchDao dao;
    private final UUID arrivalId;
    private final int amountScale;
    private final boolean v1Enabled;
    private final AtomicInteger sequence = new AtomicInteger(0);

    public SpineWriter(TxEntryBatchDao dao, UUID arrivalId, int amountScale, boolean v1Enabled) {
        this.dao = dao;
        this.arrivalId = arrivalId;
        this.amountScale = amountScale;
        this.v1Enabled = v1Enabled;
    }

    public void writeDetails(List<? extends String> lines) {
        List<TxEntryEntity> entities = new ArrayList<>(lines.size());
        for (String line : lines) {
            entities.add(toEntity(line, sequence.incrementAndGet()));
        }
        dao.batchUpsert(entities);
    }

    TxEntryEntity toEntity(String line, int seq) {
        FixedWidthLayout layout = layoutFor(line);
        OpaqueRef e2e = OpaqueRef.ofFixedWidth(layout.slice(line, "end_to_end"));
        String amountRaw = layout.slice(line, "amount");
        return TxEntryEntity.of(arrivalId, seq,
                layout.slice(line, "record_type"),
                e2e.rawBytes(), e2e.canonical(),
                layout.slice(line, "creditor_account").strip(),
                layout.slice(line, "contract_ref").strip(),
                layout.slice(line, "currency"),
                amountRaw,
                MoneyText.parse(amountRaw, amountScale),
                layout.slice(line, "branch_code").strip(),
                layout.slice(line, "debtor_name").strip(),
                layout.slice(line, "debtor_account").strip(),
                layout.length() == Layouts.DETAIL_V2.length()
                        ? layout.slice(line, "acc_type_seq") : null,
                contentHash(layout, line));
    }

    /**
     * R-41 content identity: essential business fields only, so a re-keyed
     * copy (fresh e2e, same money movement) still clashes.
     */
    static String contentHash(FixedWidthLayout layout, String line) {
        String essential = String.join("|",
                layout.slice(line, "creditor_account").strip(),
                layout.slice(line, "debtor_account").strip(),
                layout.slice(line, "branch_code").strip(),
                layout.slice(line, "amount"),
                layout.slice(line, "currency"),
                layout.slice(line, "contract_ref").strip());
        return HexFormat.of().formatHex(
                sha256().digest(essential.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    FixedWidthLayout layoutFor(String line) {
        if (line.length() == Layouts.DETAIL_V2.length()) {
            return Layouts.DETAIL_V2;
        }
        if (line.length() == Layouts.DETAIL_V1.length()) {
            if (!v1Enabled) {
                throw new FileFatalException("V1 layout fails closed in production (A-2)");
            }
            return Layouts.DETAIL_V1;
        }
        throw new FileFatalException("detail LRECL " + line.length() + " matches no layout");
    }
}
