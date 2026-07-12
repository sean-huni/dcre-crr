package za.co.fnb.dcre.crr.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.crr.data.model.TxHeaderEntity;

import java.util.UUID;

/** Guarded native UPSERT keyed by the R-05 identity (arrival_id): restarts rewrite, never duplicate. */
public interface TxHeaderRepo extends CrudRepository<TxHeaderEntity, UUID> {

    @Modifying
    @Query("""
            UPSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,
                initg_pty, business_date, client_token, layout_version)
            VALUES (:#{#e.arrivalId}, :#{#e.msgIdRaw}, :#{#e.msgId}, :#{#e.createdTs}, :#{#e.txCount},
                :#{#e.initgPty}, :#{#e.businessDate}, :#{#e.clientToken}, :#{#e.layoutVersion})""")
    void upsert(@Param("e") TxHeaderEntity e);
}
