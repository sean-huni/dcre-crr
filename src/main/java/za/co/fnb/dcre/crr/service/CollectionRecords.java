package za.co.fnb.dcre.crr.service;

import za.co.fnb.dcre.platform.copybook.FixedWidthLayout;
import za.co.fnb.dcre.platform.copybook.LayoutResolver;
import za.co.fnb.dcre.platform.copybook.Layouts;

/**
 * The shape of a collection request book, in ONE place: record 0 is the header,
 * every later record is a detail cut by its own layout. Both the header
 * pre-read and the spine writer resolve through here, so the file format is
 * described once rather than once per caller.
 *
 * <p>Record 0 is gated at the header's 109-byte CONTENT length. A generator
 * pads the header out to the detail LRECL while a real file does not, so a
 * padded header is length-identical to a V2 detail and only its INDEX tells
 * them apart; both must ingest identically (CrrJobTest.unpaddedHeaderStillIngestsAllRecords).
 *
 * <p>V1 (161) resolves here WITHOUT the fail-closed check: A-2 is a verdict on
 * the header's declared layout_version, reported by {@link HeaderService}, and
 * on the detail line by {@link SpineWriter}. Refusing it during the read would
 * report a V1 file as a length fault instead.
 */
public final class CollectionRecords implements LayoutResolver {

    public static final CollectionRecords INSTANCE = new CollectionRecords();

    private CollectionRecords() {
    }

    @Override
    public FixedWidthLayout layoutFor(final long recordIndex, final String line) {
        return recordIndex == 0 ? Layouts.HEADER : detail(line.length());
    }

    /** The detail layout for an LRECL; a length matching none is file-fatal (R-19). */
    public static FixedWidthLayout detail(final int lrecl) {
        if (lrecl == Layouts.DETAIL_V3.length()) {
            return Layouts.DETAIL_V3;
        }
        if (lrecl == Layouts.DETAIL_V2.length()) {
            return Layouts.DETAIL_V2;
        }
        if (lrecl == Layouts.DETAIL_V1.length()) {
            return Layouts.DETAIL_V1;
        }
        throw new FileFatalException("detail LRECL " + lrecl + " matches no layout");
    }
}
