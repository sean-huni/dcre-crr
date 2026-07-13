package za.co.fnb.dcre.crr.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** R-41: detail records split into contiguous [fromRecord, toRecord) ranges. */
class LineRangePartitionerTest {

    private static long from(ExecutionContext c) { return c.getLong("fromRecord"); }
    private static long to(ExecutionContext c) { return c.getLong("toRecord"); }

    @Test
    void splitsEvenlyWithRemainderOnLastPartition() {
        Map<String, ExecutionContext> parts = LineRangePartitioner.split(10, 3);
        assertEquals(3, parts.size());
        assertEquals(0, from(parts.get("partition0")));
        assertEquals(4, to(parts.get("partition0")));   // ceil(10/3)=4
        assertEquals(4, from(parts.get("partition1")));
        assertEquals(8, to(parts.get("partition1")));
        assertEquals(8, from(parts.get("partition2")));
        assertEquals(10, to(parts.get("partition2")));
    }

    @Test
    void fewerRecordsThanPartitionsCollapses() {
        Map<String, ExecutionContext> parts = LineRangePartitioner.split(2, 5);
        assertEquals(2, parts.size());
    }

    @Test
    void zeroRecordsYieldsNoPartitions() {
        assertEquals(0, LineRangePartitioner.split(0, 5).size());
    }
}
