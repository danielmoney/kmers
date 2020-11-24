package Kmers;

import Exceptions.InconsistentDataException;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KmerStream<D> implements AutoCloseable
{
    public KmerStream(Stream<KmerWithData<D>> stream, int minLength, int maxLength, boolean rc)
    {
        this.stream = stream;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public Stream<KmerWithData<D>> stream()
    {
        return stream;
    }

    public KmerStream<D> onlyStandard()
    {
        if (rc)
        {
            return filter(k -> k.getKmer().isStandard());
        }
        else
        {
            return this;
        }
    }

    public KmerStream<D> filter(Predicate<? super KmerWithData<D>> predicate)
    {
        return new KmerStream<>(stream.filter(predicate), minLength, maxLength, rc);
    }

    public void forEach(Consumer<? super KmerWithData<D>> action)
    {
        stream.forEach(action);
    }

    public KmerStream<D> map(Function<? super KmerWithData<D>, KmerWithData<D>> mapper)
    {
        return new KmerStream<>(stream.map(mapper), minLength, maxLength, rc);
    }

    public KmerStream<D> limit(long limit)
    {
        return new KmerStream<>(stream.limit(limit), minLength, maxLength, rc);
    }

    public long count()
    {
        return stream.count();
    }

    public int getMinLength()
    {
        return minLength;
    }

    public int getMaxLength()
    {
        return maxLength;
    }

    public boolean getRC()
    {
        return rc;
    }

    public void close()
    {
        stream.close();
    }

    public static <D> KmerStream<D> concatenateStreams(Stream<KmerStream<D>> streams) throws InconsistentDataException
    {
        return concatenateStreams(streams.collect(Collectors.toList()));
    }

    public static <D> KmerStream<D> concatenateStreams(Collection<KmerStream<D>> streams) throws InconsistentDataException
    {
        boolean first = true;
        int minK = 0;
        int maxK = 0;
        boolean rc = false;

        for (KmerStream<D> s: streams)
        {
            if (first)
            {
                minK = s.minLength;
                maxK = s.maxLength;
                rc = s.rc;
                first = false;
            }
            if ((s.minLength != minK) || (s.maxLength != maxK) || (s.rc != rc))
            {
                throw new InconsistentDataException("Input file diffe in Kmer parameters");
            }
        }

        if (streams.size() == 1)
        {
            return streams.iterator().next();
        }
        return new KmerStream(streams.stream().map(ks -> ks.stream).flatMap(s -> s), minK, maxK, rc);
    }

    private Stream<KmerWithData<D>> stream;
    private int minLength;
    private int maxLength;
    private boolean rc;
}
