package za.co.fnb.dcre.crr.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.crr.service.FileFatalException;
import za.co.fnb.dcre.platform.files.Layouts;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * R-41: splits the detail records of a fixed-width copybook file into
 * contiguous [fromRecord, toRecord) ranges (0-based detail index, header
 * excluded). Byte-exact: line lengths are probed as BYTE counts (never
 * charset decodes), the base offset comes from line 1 (header of any length,
 * padded or not) and the record stride from line 2 (detail LRECL + 1 newline
 * byte). A missing final newline still yields the last record; any other
 * ragged length, or a detail LRECL matching no layout, fails closed.
 */
@Component
@StepScope
public class LineRangePartitioner implements Partitioner {

    static final String FROM_RECORD = "fromRecord";
    static final String TO_RECORD = "toRecord";
    static final String LRECL = "lrecl";
    static final String BASE_OFFSET = "baseOffset";

    private final Path input;

    public LineRangePartitioner(@Value("#{jobParameters['input.file']}") String inputFile) {
        this.input = Path.of(inputFile);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        try {
            long size = Files.size(input);
            long baseOffset = lineLength(0) + 1L; // header record + its newline
            if (size <= baseOffset) {
                return Map.of(); // header-only file: no detail records
            }
            int lrecl = lineLength(baseOffset);
            if (lrecl != Layouts.DETAIL_V1.length() && lrecl != Layouts.DETAIL_V2.length()) {
                throw new FileFatalException("detail LRECL " + lrecl + " matches no layout");
            }
            long stride = lrecl + 1L;
            long detailBytes = size - baseOffset;
            long remainder = detailBytes % stride;
            if (remainder != 0 && remainder != lrecl) { // lrecl = final record without newline
                throw new FileFatalException("file length " + size + " is not a whole number of "
                        + lrecl + "-byte records after the " + (baseOffset - 1) + "-byte header");
            }
            long records = Math.ceilDiv(detailBytes, stride);
            Map<String, ExecutionContext> parts = split(records, gridSize);
            parts.values().forEach(context -> {
                context.putInt(LRECL, lrecl);
                context.putLong(BASE_OFFSET, baseOffset);
            });
            return parts;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot partition " + input, e);
        }
    }

    /** Byte count of the line starting at offset, up to \n or EOF; charset-free. */
    private int lineLength(long offset) throws IOException {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(input))) {
            in.skipNBytes(offset);
            int length = 0;
            int b;
            while ((b = in.read()) != -1 && b != '\n') {
                length++;
            }
            return length;
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
