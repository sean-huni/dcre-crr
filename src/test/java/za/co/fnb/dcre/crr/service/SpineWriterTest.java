package za.co.fnb.dcre.crr.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.platform.files.Layouts;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * R-41 content hash: covers the essential business fields only, so an
 * in-file duplicate with a freshly minted e2e still clashes, while any
 * real difference (amount) produces a distinct hash.
 */
class SpineWriterTest {

    /** First detail record of the committed V2 fixture (line 0 is the header). */
    private static final String FIXTURE_V2_LINE = firstDetailLine();

    private static String firstDetailLine() {
        try {
            return Files.readAllLines(
                    Path.of("src/test/resources/dcre_copybook_v2_dc_sample.txt")).get(1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void contentHashCoversEssentialFieldsOnly() {
        // two V2 lines differing ONLY in e2e (slice [2,37)) must clash
        String a = FIXTURE_V2_LINE;
        String b = a.substring(0, 2) + "DIFFERENTE2E".concat(" ".repeat(23)) + a.substring(37);
        assertEquals(SpineWriter.contentHash(Layouts.DETAIL_V2, a),
                SpineWriter.contentHash(Layouts.DETAIL_V2, b));
    }

    @Test
    void contentHashDiffersWhenAmountDiffers() {
        String a = FIXTURE_V2_LINE;
        // flip one digit inside the amount slice [77,92)
        StringBuilder sb = new StringBuilder(a);
        sb.setCharAt(90, a.charAt(90) == '1' ? '2' : '1');
        assertNotEquals(SpineWriter.contentHash(Layouts.DETAIL_V2, a),
                SpineWriter.contentHash(Layouts.DETAIL_V2, sb.toString()));
    }
}
