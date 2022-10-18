import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BloomFilter {

    // See `HyperLogLog.java`
    static final int EXACT_CARDINALITY = 1697;

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

        int Ms[] = { 3001, 7001, 10459, 20011 };
        for (int M : Ms) {
            System.out.println("\n== M = " + M);

            for (int i = 1; i <= 4; ++i) {
                final int nHashFuns = i;
                int approxCardinality = GZIPReader.openAndApply(filename, reader -> {
                    return computeApproximateCardinality(reader, M,
                            getSeeds(nHashFuns, 8391));
                });

                System.out.printf("est.card = %4d (%d), nhash = %d, abs.err = %4d, rel.err = %.3f\n",
                        approxCardinality, EXACT_CARDINALITY, nHashFuns,
                        EXACT_CARDINALITY - approxCardinality,
                        1.0 * EXACT_CARDINALITY / approxCardinality - 1.0);
            }
        }
    }

    static int computeApproximateCardinality(GZIPReader reader, int M,
            int seeds[]) {
        BitSet bitSet = new BitSet(M);

        int approxCardinality = 0;

        Iterator<Integer> iter = reader.intoIntTupleStream()
            .map(t -> t.b)
            .iterator();

        while (iter.hasNext()) {
            int freq = iter.next();

            // TODO implement this
            // Check whether we have seen this `freq` before by querying the
            // Bloom filter stored in `bitSet`. Use the `hashIndex(int value,
            // int maxIndex, int seed)` function.
            if (Arrays.stream(seeds).anyMatch(seed -> !bitSet.getBit(hashIndex(freq, M, seed))))
                approxCardinality++;

            // TODO implement this
            // Insert this `freq` value into the Bloom filter.
            Arrays.stream(seeds).forEach(seed -> bitSet.setBit(hashIndex(freq, M, seed), true));
        }

        return approxCardinality;
    }

    private static final ByteBuffer _hash_buffer =
        ByteBuffer.allocate(Integer.BYTES); // not thread safe!
    private static int hashIndex(int value, int maxIndex, int seed) {
        _hash_buffer.clear();
        _hash_buffer.putInt(value);
        int h = MurmurHash.hash32(_hash_buffer.array(), Integer.BYTES, seed);
        int m = h % maxIndex;
        return m < 0 ? m + maxIndex : m;
    }

    private static int[] getSeeds(int n, int seed) {
        int p = 2_147_483_647;
        int seeds[] = new int[n];
        for (int i = 0; i < n; ++i) {
            seeds[i] = ((i * seed * 92345) % p + 4_567_928) % p;
        }
        return seeds;
    }

    static class BitSet {
        BitSet(int nbits) {
            int nblocks = (nbits / 8) + ((nbits % 8 > 0) ? 1 : 0);
            blocks = new byte[nblocks];
        }

        private int getBlock(int i) { return i / 8; }
        private int getBitPos(int i) { return i % 8; }
        int size() { return blocks.length; }

        boolean getBit(int i) {
            int block = getBlock(i);
            int bitPos = getBitPos(i);
            return ((blocks[block] >> bitPos) & 0b1) == 0b1;
        }

        void setBit(int i, boolean value) {
            int block = getBlock(i);
            int bitPos = getBitPos(i);
            if (value) { blocks[block] |= (0b1 << bitPos); }
            else       { blocks[block] &=~(0b1 << bitPos); }
        }

        private byte blocks[];
    }
}
