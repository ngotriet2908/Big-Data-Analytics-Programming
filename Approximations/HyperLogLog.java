import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import java.nio.ByteBuffer;

public class HyperLogLog {

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
        System.out.println("\n== Computing exact cardinality of frequency values...");
        double exactCardinality = GZIPReader.openAndApply(filename, reader -> {
            return (double) computeExactCardinality(reader);
        });
        System.out.println("cardinality = " + exactCardinality);

        System.out.println("\n== Computing approximate cardinality of frequency values...");
        for (int b = 4; b < 16; ++b) {
            final int nbits = b; // capture in closure, must be effectively final
            double approxCardinality = GZIPReader.openAndApply(filename, reader -> {
                return computeApproximateCardinality(reader, nbits);
            });
            System.out.println("cardinality = " + approxCardinality + " for b = " + nbits);
            System.out.println("  absolute error = " +
                    Math.abs(approxCardinality - exactCardinality));
            System.out.println("  relative error = " +
                    Math.abs(approxCardinality / exactCardinality - 1.0));
            System.out.println("  typical error  = " + 1.04 / Math.sqrt(1 << b));
            System.out.println("  should lowfix  = " +
                    (approxCardinality < (5.0 / 2.0) * (1 << b)) + 
                    " (see paper p139)");
            System.out.println("  should highfix = " +
                    (approxCardinality > (1.0 / 30.0) * Math.pow(2, 32)));
            System.out.println();
        }
    }

    static int computeExactCardinality(GZIPReader reader) {
        HashSet<Integer> set = new HashSet<>();

        Iterator<Integer> iter = reader.intoIntTupleStream()
            .map(t -> t.b)
            .iterator();

        while (iter.hasNext()) {
            int freq = iter.next();
            set.add(freq);
        }

        return set.size();
    }

    static double computeApproximateCardinality(GZIPReader reader, int b) {
        Iterator<Integer> iter = reader.intoIntTupleStream()
            .map(t -> t.b)
            .iterator();

        double approxCardinality = 0.0;

        // TODO write your initialization code here
        int m = (int) Math.pow(2, b);
        int[] M = new int[m];

        for(int i = 0; i < m; i++)
            M[i] = 0;

        while (iter.hasNext()) {
            int freq = iter.next();
            int x = hash(freq);

            // System.out.println(Integer.toBinaryString(x) + " " + Integer.toBinaryString(x >>> (32-b)) + " " + Integer.toBinaryString(x << b));

            // TODO implement this
            // (1) Take the `b` most significant bits (MSBs) as index into `M`
            // (2) Replace value in `M` with position of MSB in remaining bits,
            //     if greater.

            // Tips:
            // y = x << b
            // This removes the first b bits from integer x.
            // For example 1101001011000. . . << 4 = 001011000. . .
            
            // y = x >>> (32-b)
            // This retains the first b bits from 32-bit integer x (32 bit is default in Java). For example 1101001011000. . . >>> (32 - 4) = 1101
            
            // Integer.numberOfLeadingZeros(x)
            // This returns the number of leading zeros in x.
            // For example Integer.numberOfLeadingZeros(0b001011000. . . ) = 2
            
            // hash(some integer)
            // This is a static function in the HyperLogLog class. Use this to generate a hash from an int value. 
            //This function uses the MurmurHash, for which an implementation is included in the ZIP file.
            M[x >>> (32-b)] = Math.max(M[x >>> (32-b)], Integer.numberOfLeadingZeros(x << b));
        }

        // TODO compute the estimate
        approxCardinality = getAlpha(m) * m * m * 
            Math.pow(Arrays.stream(M).mapToDouble(mj -> Math.pow(2, -mj)).sum(), -1);
        
        double E = 0.0;

        if (approxCardinality <= 5/2*m) {
            int V = (int) Arrays.stream(M).filter(mj -> mj > 0).count();
            if (V != 0) {
                E = m * Math.log(m * 1.0/V);
            } else {
                E = approxCardinality;
            }
            System.out.println(String.format("Small Range Correction E = %s, E* = %s", approxCardinality, E));
        }
        
        if (approxCardinality <= 1.0/30 * Math.pow(2, 32)) {
            E = approxCardinality;
            System.out.println(String.format("Immediate Range Correction E = %s, E* = %s", approxCardinality, E));
        } else {
            E = -Math.pow(2, 32) * Math.log(1 - approxCardinality/Math.pow(2, 32));
            System.out.println(String.format("Large Range Correction E = %s, E* = %s", approxCardinality, E));
        }

        return E;
    }

    static double getAlpha(int m) {
	if (m == 16) {
            return 0.673;
	} else if(m == 32) {
            return 0.697;
        } else if(m == 64) {
            return 0.709;
        } else if(m >= 128) {
	   return 0.7213 / (1 + 1.079 / m);	
	}
        throw new RuntimeException("getAlpha: invalid value of m");
    }

    private static final ByteBuffer _hash_buffer =
        ByteBuffer.allocate(Integer.BYTES); // not thread safe!
    private static int hash(int value) {
        _hash_buffer.clear();
        _hash_buffer.putInt(value * 11 + 1313943);
        return MurmurHash.hash32(_hash_buffer.array(), Integer.BYTES);
    }
}
