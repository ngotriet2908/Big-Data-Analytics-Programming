import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.zip.GZIPInputStream;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Consumer;

/**
 * Read GZIP'ed data files of int tuples.
 */
public class GZIPReader implements Closeable {
    public static class IntTuple {
        public IntTuple(int a, int b) {
            this.a = a;
            this.b = b;
        }

        public String toString() { return "(" + a + ',' + b + ')'; }
        
        public final int a;
        public final int b;
    }

    private static final Function<String, IntTuple> INT_TUPLE_PARSER = (String line) -> {
        String[] splits = line.split("\\s+");
        if (splits.length != 2) 
            throw new RuntimeException("wrong tuple length");

        int a = Integer.parseInt(splits[0]);
        int b = Integer.parseInt(splits[1]);
        return new IntTuple(a, b);
    };

    private static final Predicate<String> COMMENT_FILTER = (String line) -> {
        return !line.startsWith("#");
    };

    public GZIPReader(String filename) throws IOException {
        reader = new BufferedReader(new InputStreamReader(new
                    GZIPInputStream(new FileInputStream(filename))));
    }

    public static <R> R openAndApply(String filename, Function<GZIPReader, R> fn)
            throws Exception {
        try (GZIPReader reader = new GZIPReader(filename)) {
            return fn.apply(reader);
        }
    }

    public static void openAndConsume(String filename, Consumer<GZIPReader> fn)
            throws Exception {
        try (GZIPReader reader = new GZIPReader(filename)) {
            fn.accept(reader);
        }
    }

    public <T> Stream<T> intoStream(Function<String, T> parser) {
        return reader.lines()
            .filter(COMMENT_FILTER)
            .map(parser);
    }

    public Stream<IntTuple> intoIntTupleStream() {
        return intoStream(INT_TUPLE_PARSER);
    }

    public void close() throws IOException {
        reader.close();
    }

    private BufferedReader reader;
}
