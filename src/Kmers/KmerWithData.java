package Kmers;

import Exceptions.InvalidBaseException;
import Compression.Compressor;

import java.util.Arrays;
import java.util.function.Function;

public class KmerWithData<D> implements Comparable<KmerWithData<D>>
{
    public KmerWithData(Kmer kmer, D data)
    {
        this.kmer = kmer;
        this.data = data;
    }

    public Kmer getKmer()
    {
        return kmer;
    }

    public D getData()
    {
        return data;
    }

    public String toString()
    {
        return kmer + "\t" + data;
    }

    public KmerWithData<D> limitTo(int length)
    {
        return new KmerWithData<D>(kmer.limitTo(length),data);
    }

    public int compareTo(KmerWithData<D> o)
    {
        int c = kmer.compareTo(o.getKmer());
        if (c != 0)
        {
            return c;
        }
        else
        {
            return Integer.compare(data.hashCode(),o.getData().hashCode());
        }
    }

    private Kmer kmer;
    private D data;
}
