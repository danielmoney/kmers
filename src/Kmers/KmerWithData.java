package Kmers;

import Exceptions.InvalidBaseException;
import Compression.Compressor;

import java.util.Arrays;
import java.util.function.Function;

public class KmerWithData<D> implements Comparable<KmerWithData<D>>
{
    public KmerWithData(String s, Function<String,D> mapper) throws InvalidBaseException
    {
        kmer = new Kmer(s.split("\t")[0]);
        data = mapper.apply(s.split("\t")[1]);
    }

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

    public byte[] compressedBytes(Compressor<D> compressor)
    {
        byte[] kb = kmer.compressedBytes();
        byte[] db = compressor.compress(data);

        byte[] b = new byte[kb.length + db.length];
        System.arraycopy(kb,0,b,0,kb.length);
        System.arraycopy(db,0,b,kb.length,db.length);

        return b;
    }

    public KmerWithData<D> limitTo(int length)
    {
        return new KmerWithData<D>(kmer.limitTo(length),data);
    }

    public static <D> KmerWithData<D> createUnchecked(String s, Function<String, D> mapper)
    {
        KmerWithData<D> k = null;
        try
        {
            k = new KmerWithData<>(s, mapper);
        }
        catch (InvalidBaseException ex)
        {
            return null;
        }
        return k;
    }

    public static <D> KmerWithData<D> createFromCompressed(byte[] bytes, Compressor<D> compressor)
    {
        int len = bytes[0];
        int l = (len - 1) / 4 + 1;

        Kmer k = Kmer.createFromCompressed(Arrays.copyOfRange(bytes,0,l+1));
        D d = compressor.decompress(Arrays.copyOfRange(bytes,l+1,bytes.length));

        return new KmerWithData<D>(k,d);
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
