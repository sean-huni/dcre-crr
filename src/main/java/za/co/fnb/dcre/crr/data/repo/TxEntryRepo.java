package za.co.fnb.dcre.crr.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crr.data.model.TxEntryEntity;

import java.util.UUID;

/** Guarded native UPSERT keyed (arrival_id, sequence): the R-05 record identity. */
public interface TxEntryRepo extends CrudRepository<TxEntryEntity, UUID> {

    @Modifying
    @Query("""
            INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e, creditor_account, contract_ref, currency, amount_raw, amount, branch_code, debtor_name, debtor_account, acc_type_seq)
            VALUES (:#{#e.arrivalId}, :#{#e.sequence}, :#{#e.recordType}, :#{#e.e2eRaw}, :#{#e.e2e}, :#{#e.creditorAccount}, :#{#e.contractRef}, :#{#e.currency}, :#{#e.amountRaw}, :#{#e.amount}, :#{#e.branchCode}, :#{#e.debtorName}, :#{#e.debtorAccount}, :#{#e.accTypeSeq})
            ON CONFLICT (arrival_id, sequence) DO UPDATE SET record_type = EXCLUDED.record_type, e2e_raw = EXCLUDED.e2e_raw, e2e = EXCLUDED.e2e, creditor_account = EXCLUDED.creditor_account, contract_ref = EXCLUDED.contract_ref, currency = EXCLUDED.currency, amount_raw = EXCLUDED.amount_raw, amount = EXCLUDED.amount, branch_code = EXCLUDED.branch_code, debtor_name = EXCLUDED.debtor_name, debtor_account = EXCLUDED.debtor_account, acc_type_seq = EXCLUDED.acc_type_seq""")
    void upsert(@Param("e") TxEntryEntity e);

    long countByArrivalId(UUID arrivalId);
}
