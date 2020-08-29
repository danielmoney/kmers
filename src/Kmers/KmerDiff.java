package Kmers;

import Exceptions.InvalidBaseException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class KmerDiff
{
    public KmerDiff(Kmer k1, Kmer k2)
    {
        byte[] b1 = k1.getRawBytes();
        byte[] b2 = k2.getRawBytes();

        diffs = new LinkedList<>();

        for (int i = 0; i < b1.length; i++)
        {
            if (b1[i] != b2[i])
            {
                try
                {
                    diffs.add(new Diff((byte) i,Base.fromByte(b2[i])));
                }
                catch (InvalidBaseException e)
                {
                    // Should never get here since we know they're valid
                }
            }
        }
    }

    public KmerDiff(List<Diff> diffs)
    {
        this.diffs = diffs;
    }

    public Kmer apply(Kmer k) throws InvalidBaseException
    {
        byte[] b = Arrays.copyOf(k.getRawBytes(),k.getRawBytes().length);
        for (Diff d: diffs)
        {
            b[d.position] = d.newBase.pos();
        }
        return new Kmer(b);
    }

    public List<Diff> getDiffs()
    {
        return diffs;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (Diff d: diffs)
        {
            sb.append(d.position);
            sb.append(d.newBase.toString());
        }
        if (diffs.size() == 0)
        {
            sb.append("-");
        }
        return sb.toString();
    }

    public int dist()
    {
        return diffs.size();
    }

    private List<Diff> diffs;

    public static class Diff
    {
        public Diff(byte position, Base newBase)
        {
            this.position = position;
            this.newBase = newBase;
        }

        public byte getPosition()
        {
            return position;
        }

        public Base getBase()
        {
            return newBase;
        }

        private byte position;
        private Base newBase;
    }
}
