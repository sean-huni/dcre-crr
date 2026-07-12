package za.co.fnb.dcre.crr.service;

import org.springframework.jdbc.core.JdbcTemplate;
import za.co.fnb.dcre.platform.files.FixedWidthLayout;
import za.co.fnb.dcre.platform.files.Layouts;
import za.co.fnb.dcre.platform.model.MoneyText;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Persists detail lines into tx_entry. Upsert keyed (arrival_id, sequence):
 * a restarted chunk rewrites identical rows, never duplicates (R-05).
 * V1 (161) fails closed unless dcre.v1-enabled (A-2).
 */
public class SpineWriter {

    private final JdbcTemplate jdbc;
    private final UUID arrivalId;
    private final int amountScale;
    private final boolean v1Enabled;
    private final AtomicInteger sequence = new AtomicInteger(0);

    public SpineWriter(JdbcTemplate jdbc, UUID arrivalId, int amountScale, boolean v1Enabled) {
        this.jdbc = jdbc;
        this.arrivalId = arrivalId;
        this.amountScale = amountScale;
        this.v1Enabled = v1Enabled;
    }

    public void writeDetails(List<? extends String> lines) {
        for (String line : lines) {
            writeDetail(line, sequence.incrementAndGet());
        }
    }

    void writeDetail(String line, int seq) {
        FixedWidthLayout layout = layoutFor(line);
        OpaqueRef e2e = OpaqueRef.ofFixedWidth(layout.slice(line, "end_to_end"));
        String amountRaw = layout.slice(line, "amount");
        jdbc.update("""
                UPSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,
                    creditor_account, contract_ref, currency, amount_raw, amount,
                    branch_code, debtor_name, debtor_account, acc_type_seq)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                arrivalId, seq,
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
                        ? layout.slice(line, "acc_type_seq") : null);
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
