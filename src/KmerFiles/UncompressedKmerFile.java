package KmerFiles;

import Files.IndexedInputFile;
import Files.NoSuchKeyException;
import Kmers.Kmer;
import Kmers.KmerWithData;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Stream;

public class UncompressedKmerFile<D> extends KmerFile<D>
{
    public UncompressedKmerFile(IndexedInputFile file, Function<String,D> mapper) throws IOException
    {
        super(file);
    }

    public Stream<KmerWithData<D>> kmers(int key)
    {
        if (file.hasIndex(key))
        {
            // SHOULD PROBABLY CHANGE TO A SPLITERATOR!!
            //Hacky but neccessary way to get round stream issues
            final Kmer[] prev = new Kmer[1];
            prev[0] = null;

            return file.lines(key).map(l ->
                    {
                        String[] parts = l.split("\t");
                        Kmer kmer = Kmer.createUnchecked(prev[0], parts[0]);
                        D data = mapper.apply(parts[1]);
                        prev[0] = kmer;
                        return new KmerWithData<>(kmer, data);
                    }
            );
        }
        else
        {
            return Stream.empty();
        }
    }

    private Function<String,D> mapper;
}
