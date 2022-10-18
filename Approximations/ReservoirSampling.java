import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Random;



public class ReservoirSampling {

    public static void main(String[] args) {
        String filename = "./enwiki-2013-frequencies.txt.gz";

        try { runExperiments(filename); }
        catch (Exception e) { dealWithException(e); }
    }

    static void dealWithException(Exception e) {
        System.err.println("!Exception: " + e.getMessage());
        e.printStackTrace();
    }

    static void runExperiments(String filename) throws Exception {
        System.out.println("\n== Experiment 1: getting started");
        GZIPReader.openAndConsume(filename, ReservoirSampling::printFirstValues);

        System.out.println("\n== Experiment 2: exact");
        GZIPReader.openAndConsume(filename, ReservoirSampling::printExactMean);

        System.out.println("\n== Experiment 3: approx");
        Random rng = new Random(12);
        for (int i = 0; i < 8; ++i) {
            final int nSamples = 1024 << i;
            GZIPReader.openAndConsume(filename, reader -> {
                printApproximateMean(reader, nSamples, rng);
            });
        }
    }

    static void printFirstValues(GZIPReader reader) {
        System.out.println("The first 10 items in the dataset:");
        Stream<GZIPReader.IntTuple> s = reader.intoIntTupleStream();
        s.limit(10).forEach(System.out::println);
    }

    static void printExactMean(GZIPReader reader) {
        long tStart = System.currentTimeMillis();

        Iterator<Integer> iter = reader.intoIntTupleStream()
            .map(t -> t.b)
            .iterator();

        int n = 0;
        int sum = 0;
        while (iter.hasNext()) {
            sum += iter.next();
            n += 1;
        }

        long tStop = System.currentTimeMillis();
        System.out.println("n = " + n + ", sum = " + sum + ", avg(links) = " +
                (sum * 1.0 / n) + " (time: " + ((tStop-tStart)/1000.0) + "s)");
    }

    static void printApproximateMean(GZIPReader reader, int nSamples, Random rng) {
        long tStart = System.currentTimeMillis();

        int[] freqSamples = new int[nSamples];
        Iterator<Integer> iter = reader.intoIntTupleStream()
            .map(t -> t.b)
            .iterator();

        // Reservoir sampling:
        int nLine = 0;
        for (; iter.hasNext(); ++nLine) {
            int freq = iter.next();
            // TODO Implement this
            // Two steps: (1) We haven't seen `nSamples` values yet.
            //            (2) We have seen >= `nSamples` values and we replace
            //                a previous value with probability k/n. Use
            //                `rng.nextInt(...)`.
            if (nLine < freqSamples.length) {
                freqSamples[nLine] = freq;
            } else {
                int ran = rng.nextInt(nLine);
                if (ran < nSamples)
                    freqSamples[ran] = freq;
            }
        }

        // Compute statistic:
        int sum = 0;
        for (int freq : freqSamples) {
            sum += freq;
        }

        long tStop = System.currentTimeMillis();
        System.out.println("nSamples = " + nSamples + ", sum = " + sum + 
                ", avg(links) = " + (sum * 1.0 / nSamples) + " (time: " +
                ((tStop-tStart)/1000.0) + "s)");
    }

}
