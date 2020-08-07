package Kmers;

import Exceptions.InvalidBaseException;

public class StandardKmer extends Kmer
{

    public StandardKmer(String s) throws InvalidBaseException
    {
        super(s);
        setConsistent(chars);
        standard = KmerStandard.STANDARD;
    }

    public StandardKmer(byte[] chars) throws InvalidBaseException
    {
        super(chars);
        setConsistent(chars);
        standard = KmerStandard.STANDARD;
    }

    public StandardKmer(Kmer kmer)
    {
        super(kmer);
        setConsistent(chars);
        standard = KmerStandard.STANDARD;
    }

    public StandardKmer(Kmer kmer, String s) throws InvalidBaseException
    {
        super(kmer, s);
        setConsistent(chars);
        standard = KmerStandard.STANDARD;
    }

    private void setConsistent(byte[] orig)
    {
        byte[] rc = rc(orig);

        for (int i = 0; i < orig.length; i++)
        {
            if (orig[i] > rc[i])
            {
                chars = rc;
                break;
            }
        }
    }

    public static StandardKmer createUnchecked(byte[] chars)
    {
        StandardKmer k = null;
        try
        {
            k = new StandardKmer(chars);
        }
        catch (InvalidBaseException ex)
        {
            System.err.println("Hmmm");
        }
        return k;
    }
}
