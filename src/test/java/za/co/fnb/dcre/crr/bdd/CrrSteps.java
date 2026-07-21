package za.co.fnb.dcre.crr.bdd;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step definitions for the CRR boundary reader. Input files are derived from
 * the committed V2/V1 samples; header mutations reuse the Layouts offsets
 * (tx_count occupies columns 52-66, header content length 109).
 */
public class CrrSteps {

    static final String V2_SAMPLE = "dcre_copybook_v2_dc_sample.txt";
    static final String V1_SAMPLE = "dcre_copybook_v1_legacy_sample.txt";
    static final String V2_NAME = "FNBRF01_DCRERF2026071112000002.txt";
    static final String V1_NAME = "FNBRF01_DCRERF2026071112000001.txt";
    static final int TX_COUNT_START = 52;
    static final int TX_COUNT_END = 66;

    @Autowired
    Job crrJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    UUID arrival;
    Path inputFile;
    String originalName;
    JobExecution execution;

    @Given("the boundary reader receives the standard V2 collection file")
    public void standardV2File() throws Exception {
        arrival = UUID.randomUUID();
        inputFile = sample(V2_SAMPLE);
        originalName = V2_NAME;
    }

    @Given("the boundary reader receives the legacy V1 collection file")
    public void legacyV1File() throws Exception {
        arrival = UUID.randomUUID();
        inputFile = sample(V1_SAMPLE);
        originalName = V1_NAME;
    }

    @Given("the boundary reader receives the V2 file with a header declaring {int} transactions")
    public void v2FileWithWrongCount(int declared) throws Exception {
        arrival = UUID.randomUUID();
        List<String> lines = Files.readAllLines(sample(V2_SAMPLE));
        String header = lines.get(0);
        String mutated = header.substring(0, TX_COUNT_START)
                + String.format("%014d", declared)
                + header.substring(TX_COUNT_END);
        inputFile = writeInput("wrong-count", mutated, lines.subList(1, lines.size()));
        originalName = V2_NAME;
    }

    @Given("the boundary reader receives the V2 file with a truncated header")
    public void v2FileWithTruncatedHeader() throws Exception {
        arrival = UUID.randomUUID();
        List<String> lines = Files.readAllLines(sample(V2_SAMPLE));
        inputFile = writeInput("short-header", lines.get(0).substring(0, 80), lines.subList(1, lines.size()));
        originalName = V2_NAME;
    }

    @Given("the file arrived under the name {string}")
    public void arrivedUnderName(String name) {
        originalName = name;
    }

    @When("the CRR job runs")
    public void crrJobRuns() throws Exception {
        execution = jobOperator.start(crrJob, params(null));
    }

    @When("the CRR job runs again for the same arrival")
    public void crrJobRunsAgain() throws Exception {
        // new attempt parameter: a fresh JobInstance re-processing the same
        // arrival exercises the ON CONFLICT upsert path, not instance refusal
        execution = jobOperator.start(crrJob, params("2"));
    }

    @When("the CRR job runs with the launch flow {string}")
    public void crrJobRunsWithFlow(String flow) throws Exception {
        // SCRUM-69: AGT passes flow=PAY for onhost-req-endo arrivals; the
        // param is non-identifying, mirroring input.file/original.name.
        JobParameters withFlow = new JobParametersBuilder(params(null))
                .addString("flow", flow, false)
                .toJobParameters();
        execution = jobOperator.start(crrJob, withFlow);
    }

    @Then("the job completes with a clean business verdict")
    public void jobCompletesClean() {
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        assertFalse(execution.getExecutionContext().containsKey("fileFatalReason"),
                "no file-fatal reason expected");
    }

    @Then("the file is rejected file-fatally with a reason containing {string}")
    public void rejectedFileFatally(String reasonPart) {
        // FILE_FATAL is a business verdict, not a crash: the job COMPLETES
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        String reason = execution.getExecutionContext().getString("fileFatalReason");
        assertTrue(reason.contains(reasonPart),
                "expected file-fatal reason containing '" + reasonPart + "' but was: " + reason);
    }

    @Then("the spine holds one header row and {int} entry rows for the arrival")
    public void spineHoldsRows(int entries) {
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM tx_header WHERE arrival_id=?", Integer.class, arrival));
        assertEquals(entries, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, arrival));
    }

    @Then("the header row is stamped with flow {string}")
    public void headerStampedWithFlow(String flow) {
        assertEquals(flow, jdbc.queryForObject(
                "SELECT flow FROM tx_header WHERE arrival_id=?", String.class, arrival));
    }

    @Then("no spine entries are persisted for the arrival")
    public void noSpineEntries() {
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, arrival));
    }

    @Then("every persisted amount equals its raw digits scaled to two decimals")
    public void amountsMatchRawDigits() {
        List<java.util.Map<String, Object>> rows = jdbc.queryForList(
                "SELECT amount, amount_raw FROM tx_entry WHERE arrival_id=?", arrival);
        assertFalse(rows.isEmpty(), "expected persisted entries");
        for (var row : rows) {
            BigDecimal amount = (BigDecimal) row.get("amount");
            String raw = (String) row.get("amount_raw");
            assertEquals(new BigDecimal(raw).movePointLeft(2).stripTrailingZeros(),
                    amount.stripTrailingZeros(),
                    "amount parsed via the single MoneyText converter at scale 2");
        }
    }

    JobParameters params(String attempt) {
        JobParametersBuilder builder = new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("input.file", inputFile.toAbsolutePath().toString(), false)
                .addString("original.name", originalName, false);
        if (attempt != null) {
            builder.addString("attempt", attempt, true);
        }
        return builder.toJobParameters();
    }

    Path sample(String name) {
        return Path.of("src/test/resources", name).toAbsolutePath();
    }

    Path writeInput(String label, String header, List<String> details) throws Exception {
        Path dir = Path.of("build/bdd-input");
        Files.createDirectories(dir);
        Path file = dir.resolve(label + "-" + arrival + ".txt");
        Files.write(file, concat(header, details));
        return file.toAbsolutePath();
    }

    static List<String> concat(String header, List<String> details) {
        java.util.ArrayList<String> all = new java.util.ArrayList<>();
        all.add(header);
        all.addAll(details);
        return all;
    }
}
