package za.co.fnb.dcre.crr.service;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Review m1 (SCRUM-69): the flow launch parameter is a closed vocabulary.
 * COL and PAY are the only stampable flows (absent/blank defaults to COL);
 * anything else is a launcher misconfiguration and must fail the job, not
 * silently stamp an unknown flow that no eligibility arm would ever pick up.
 */
class HeaderServiceFlowTest {

    @Test
    void unknownFlowFailsClosedBeforeAnyIo() {
        final HeaderService service = new HeaderService(null, false);
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> service.ingestHeader(UUID.randomUUID(),
                        Path.of("build/does-not-exist.txt"), "FNBRF01_X.txt", "MAN"));
        assertTrue(e.getMessage().contains("MAN"), "got: " + e.getMessage());
    }
}
