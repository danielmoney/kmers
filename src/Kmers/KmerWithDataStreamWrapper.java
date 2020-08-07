package Kmers;

import java.util.stream.Stream;

public class KmerWithDataStreamWrapper<D>
{
    public KmerWithDataStreamWrapper(Stream<KmerWithData<D>> stream, int minLength, int maxLength)
    {
        this.stream = stream;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public Stream<KmerWithData<D>> stream()
    {
        return stream;
    }

    public int getMinLength()
    {
        return minLength;
    }

    public int getMaxLength()
    {
        return maxLength;
    }

    public KmerWithDataStreamWrapper<D> newStream(Stream<KmerWithData<D>> stream)
    {
        return new KmerWithDataStreamWrapper<>(stream,minLength,maxLength);
    }

    private Stream<KmerWithData<D>> stream;
    private int minLength;
    private int maxLength;
}
