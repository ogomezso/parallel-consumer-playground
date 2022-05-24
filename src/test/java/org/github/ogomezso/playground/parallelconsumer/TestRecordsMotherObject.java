package org.github.ogomezso.playground.parallelconsumer;

import java.util.Arrays;
import java.util.List;

import es.santander.kafka.test.objects.TestRecord;

public class TestRecordsMotherObject {

    public static List<TestRecord<String, String>> createTestRecords() {
        return Arrays.asList(
                TestRecord.<String, String>builder().key("1").value("1.b").build(),
                TestRecord.<String, String>builder().key("1").value("1.a").build(),
                TestRecord.<String, String>builder().key("1").value("1.c").build(),
                TestRecord.<String, String>builder().key("1").value("1.d").build(),
                TestRecord.<String, String>builder().key("1").value("1.e").build(),
                TestRecord.<String, String>builder().key("2").value("2.a").build(),
                TestRecord.<String, String>builder().key("2").value("2.b").build(),
                TestRecord.<String, String>builder().key("2").value("2.c").build(),
                TestRecord.<String, String>builder().key("2").value("2.d").build(),
                TestRecord.<String, String>builder().key("2").value("2.e").build(),
                TestRecord.<String, String>builder().key("3").value("3.a").build(),
                TestRecord.<String, String>builder().key("3").value("3.b").build(),
                TestRecord.<String, String>builder().key("3").value("3.c").build(),
                TestRecord.<String, String>builder().key("3").value("3.d").build(),
                TestRecord.<String, String>builder().key("3").value("3.e").build(),
                TestRecord.<String, String>builder().key("4").value("4.a").build(),
                TestRecord.<String, String>builder().key("4").value("4.b").build(),
                TestRecord.<String, String>builder().key("4").value("4.c").build(),
                TestRecord.<String, String>builder().key("4").value("4.d").build(),
                TestRecord.<String, String>builder().key("4").value("4.e").build());
    }
}
