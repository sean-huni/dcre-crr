package za.co.fnb.dcre.crr.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * R-41: splits the detail records of a fixed-width copybook file into
 * contiguous [fromRecord, toRecord) ranges (0-based detail index, header
 * excluded). Record length on disk = detail LRECL + 1 newline byte; the
 * generator pads the header record to the detail LRECL, so the LRECL is
 * derived from the first line and the record count from the file length
 * (verified against the committed V2 fixture: 31 x 170 = 5270 bytes).
 */
@Component
@StepScope
public class LineRangePartitioner implements Partitioner {

    static final String FROM_RECORD = "fromRecord";
    static final String TO_RECORD = "toRecord";
    static final String LRECL = "lrecl";

    private final Path input;

    public LineRangePartitioner(@Value("#{jobParameters['input.file']}") String inputFile) {
        this.input = Path.of(inputFile);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        try {
            int lrecl = detailLrecl();
            long records = Files.size(input) / (lrecl + 1) - 1; // header record excluded
            Map<String, ExecutionContext> parts = split(records, gridSize);
            parts.values().forEach(context -> context.putInt(LRECL, lrecl));
            return parts;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot partition " + input, e);
        }
    }

    /** Header record is padded to the detail LRECL, so line 1's length IS the LRECL. */
    private int detailLrecl() throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(input)) {
            String first = reader.readLine();
            if (first == null || first.isEmpty()) {
                throw new IOException("empty file " + input);
            }
            return first.length();
        }
    }

    /** Pure split: chunks of ceil(records/gridSize), remainder on the last partition. */
    static Map<String, ExecutionContext> split(long records, int gridSize) {
        Map<String, ExecutionContext> parts = new LinkedHashMap<>();
        if (records <= 0 || gridSize <= 0) {
            return parts;
        }
        long chunk = (records + gridSize - 1) / gridSize;
        int index = 0;
        for (long from = 0; from < records; from += chunk, index++) {
            ExecutionContext context = new ExecutionContext();
            context.putLong(FROM_RECORD, from);
            context.putLong(TO_RECORD, Math.min(from + chunk, records));
            parts.put("partition" + index, context);
        }
        return parts;
    }
}
