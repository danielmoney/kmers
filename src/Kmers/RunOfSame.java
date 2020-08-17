package Kmers;

import java.util.function.Predicate;

public class RunOfSame implements Predicate<KmerWithData<?>>
{
    public RunOfSame(int length)
    {
        this.length = length;
    }

    public boolean test(KmerWithData<?> kwd)
    {
        Kmer k = kwd.getKmer();

        byte c = k.chars[0];
        byte l = 1;

        for (int i = 1; i < k.chars.length; i++)
        {
            if (k.chars[i] == c)
            {
                l++;
                if (l == length)
                {
                    return false;
                }
            }
            else
            {
                c = k.chars[i];
                l = 1;
            }
        }
        return true;
    }

    private int length;
}
