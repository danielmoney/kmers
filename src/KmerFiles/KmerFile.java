package KmerFiles;

import Exceptions.InvalidBaseException;
import Files.IndexedInputFile;
import Files.NoSuchKeyException;
import Kmers.Kmer;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Streams.StreamUtils;

import java.io.IOException;
import java.util.function.BinaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.DataFormatException;

public abstract class KmerFile<D>
{
    public KmerFile(IndexedInputFile file)
    {
        this.file = file;
    }

    public abstract Stream<KmerWithData<D>> kmers(int key);

    public Stream<KmerWithData<D>> allKmers() throws IOException
    {
        return IntStream.range(0, file.maxIndex()).filter(i -> file.hasIndex(i)).mapToObj(i ->
                kmers(i)).flatMap(s -> s);
    }

    public Stream<KmerWithData<D>> restrictedKmers(int key, int minLength, int maxLength, BinaryOperator<KmerWithData<D>> reducer) throws IOException
    {
        return KmerUtils.restrictedStream(kmers(key),minLength,maxLength,reducer);
    }

    public Stream<KmerWithData<D>> allRestrictedKmers(int minLength, int maxLength, BinaryOperator<KmerWithData<D>> reducer) throws IOException
    {
        return KmerUtils.restrictedStream(allKmers(),minLength,maxLength,reducer);
    }

    protected IndexedInputFile file;
}

