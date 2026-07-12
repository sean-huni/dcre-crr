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
            UPSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,
                creditor_account, contract_ref, currency, amount_raw, amount,
                branch_code, debtor_name, debtor_account, acc_type_seq)
            VALUES (:#{#e.arrivalId}, :#{#e.sequence}, :#{#e.recordType}, :#{#e.e2eRaw}, :#{#e.e2e},
                :#{#e.creditorAccount}, :#{#e.contractRef}, :#{#e.currency}, :#{#e.amountRaw}, :#{#e.amount},
                :#{#e.branchCode}, :#{#e.debtorName}, :#{#e.debtorAccount}, :#{#e.accTypeSeq})""")
    void upsert(@Param("e") TxEntryEntity e);

    long countByArrivalId(UUID arrivalId);
}
