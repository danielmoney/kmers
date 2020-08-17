package Kmers;

import java.util.function.Predicate;

public class Dust implements Predicate<KmerWithData<?>>
{
    public Dust(int threshold)
    {
        this.threshold = threshold;
    }

    public boolean test(KmerWithData<?> kwd)
    {
        Kmer k = kwd.getKmer();

        int[] counts = new int[64];

        for (int i = 0; i < k.length() - 2; i++)
        {
            counts[k.chars[i] * 16 + k.chars[i+1] * 4 + k.chars[i+2]]++;
        }

        int score = 0;
        for (int i = 0; i < 64; i++)
        {
            score += (counts[i] * (counts[i] - 1));
        }

        return score < (threshold * k.length() * 2);
    }

    private int threshold;
}
