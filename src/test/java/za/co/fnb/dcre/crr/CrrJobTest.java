package za.co.fnb.dcre.crr;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CrrJobTest {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
        // second database for batch metadata on the same container
        registry.add("crr.batch.datasource.url",
                () -> CRDB.getJdbcUrl().replace("/" + CRDB.getDatabaseName(), "/crr_meta"));
    }

    @Autowired
    Job crrJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    JdbcTemplate jdbc;

    static final UUID ARRIVAL = UUID.randomUUID();

    static {
        // create the metadata database before the context wires the batch DS
        try (var conn = java.sql.DriverManager.getConnection(CRDB.getJdbcUrl(), CRDB.getUsername(), CRDB.getPassword())) {
            conn.createStatement().execute("CREATE DATABASE IF NOT EXISTS crr_meta");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    JobParameters params(UUID arrival, String file, String name) {
        return new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("input.file", Path.of("src/test/resources", file).toAbsolutePath().toString(), false)
                .addString("original.name", name, false)
                .toJobParameters();
    }

    @Test
    @Order(1)
    void parsesDcSampleIntoSpine() throws Exception {
        JobExecution run = jobOperator.start(crrJob,
                params(ARRIVAL, "dcre_copybook_v2_dc_sample.txt", "FNBRF01_DCRERF2026071112000002.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tx_header WHERE arrival_id=?", Integer.class, ARRIVAL));
        assertEquals(30, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, ARRIVAL));
        BigDecimal amount = jdbc.queryForObject(
                "SELECT amount FROM tx_entry WHERE arrival_id=? AND sequence=1", BigDecimal.class, ARRIVAL);
        String raw = jdbc.queryForObject(
                "SELECT amount_raw FROM tx_entry WHERE arrival_id=? AND sequence=1", String.class, ARRIVAL);
        assertEquals(new BigDecimal(raw).movePointLeft(2).stripTrailingZeros(), amount.stripTrailingZeros(),
                "amount parsed via the single MoneyText converter at scale 2");
    }

    @Test
    @Order(2)
    void rerunSameIdentityDoesNotDuplicate() {
        // R-05/R-16: same identifying parameters = same JobInstance; a completed
        // instance refuses a second run and the spine stays exactly 30 rows.
        var thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () ->
                jobOperator.start(crrJob,
                        params(ARRIVAL, "dcre_copybook_v2_dc_sample.txt", "FNBRF01_DCRERF2026071112000002.txt")));
        assertTrue(thrown.getClass().getSimpleName().contains("JobInstanceAlreadyComplete")
                        || String.valueOf(thrown.getMessage()).contains("already"),
                "unexpected: " + thrown);
        assertEquals(30, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, ARRIVAL));
    }

    @Test
    @Order(3)
    void v1FailsClosedAsFileFatal() throws Exception {
        UUID v1Arrival = UUID.randomUUID();
        JobExecution run = jobOperator.start(crrJob,
                params(v1Arrival, "dcre_copybook_v1_legacy_sample.txt", "FNBRF01_DCRERF2026071112000001.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus(), "FILE_FATAL is a business verdict, not a crash");
        assertTrue(run.getExecutionContext().containsKey("fileFatalReason"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, v1Arrival),
                "no details persisted for a file-fatal V1 file");
    }
}
