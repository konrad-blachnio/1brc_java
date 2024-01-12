/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


/*
 * Baseline:
 *   Run took: 317s (317371 millis), Run took: 344s (344601 millis), Run took: 313s (313099 millis)
 * Buffered Reader:
 *   BUFFER_SIZE = 8 * 1024 -> Run took: 95s (95365 millis)
 *   BUFFER_SIZE = 1024 * 1024 -> Run took: 84s (84244 millis)
 *   BUFFER_SIZE = 10 * 1024 * 1024 -> Run took: 91s (91515 millis)
 *
 */
public class CalculateAverage_konrad {

//    private static final String FILE = "./measurements.txt";
    private static final String FILE = "./measurements_10.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    static int lineNo = 0;

    final static Charset utf8 = StandardCharsets.UTF_8;
    final static char nl = ';';

    private static void processPart(final ByteBuffer bb, final int read) {
        bb.position(0);//we reset the buffer position so we can read from it
        final CharBuffer cb = utf8.decode(bb);
        final char[] arr = cb.array();
        final String str = new String(arr);

        for (int i = 0; i < read; i++) {
            if (arr[i] == nl) {
                lineNo++;
            }
            System.out.print(arr[i]);
        }
    }



    private static void privMain(String[] args) throws IOException {
        final FileChannel fc = FileChannel.open(Path.of(FILE), StandardOpenOption.READ);
        final ByteBuffer bb = ByteBuffer.allocate(1024);

        int readBytes = fc.read(bb);
        while (readBytes > 0) {
            processPart(bb, readBytes);
            bb.clear();
            readBytes = fc.read(bb);
        }

        System.out.println("Lines: " + lineNo);
    }

    public static void main(final String[] args) throws IOException {
        final long start = System.currentTimeMillis();
        privMain(args);
        final long finish = System.currentTimeMillis();
        System.out.println("Run took: " + (finish - start)/1000 + "s (" + (finish - start) + " millis)");
    }
}
