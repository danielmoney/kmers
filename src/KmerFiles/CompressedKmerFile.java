package KmerFiles;

import Compression.Compressor;
import Files.IndexedInputFile;
import Files.NoSuchKeyException;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Streams.StreamUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Spliterator;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CompressedKmerFile<D> extends KmerFile<D>
{
    public CompressedKmerFile(IndexedInputFile file, Compressor<D> compressor)
    {
        super(file);
        this.compressor = compressor;
    }

    public Stream<KmerWithData<D>> kmers(int key)
    {
        return StreamSupport.stream(new CompressedKmerSpliterator<>(file.data(key), compressor), false);
    }

    private Compressor<D> compressor;

    public class CompressedKmerSpliterator<D> implements Spliterator<KmerWithData<D>>
    {
        public CompressedKmerSpliterator(byte[] bytes, Compressor<D> compressor)
        {
            this.bytes = ByteBuffer.wrap(bytes);
            this.compressor = compressor;
            prevkb = new byte[0];
        }

        @Override
        public boolean tryAdvance(Consumer<? super KmerWithData<D>> consumer)
        {
            if (bytes.hasRemaining())
            {
                byte copy = bytes.get();

                byte len;
                if (copy == 0)
                {
                    len = bytes.get();
                }
                else
                {
                    len = prevkb[0];
                }

                int l = (len - 1) / 4 + 1;
                byte[] kb = new byte[l+1];

                if (copy > 0)
                {
                    System.arraycopy(prevkb,0,kb,0,copy);
                    bytes.get(kb,copy,l-copy+1);
                }
                else
                {
                    kb[0] = len;
                    bytes.get(kb,1,l);
                }

                prevkb = kb;
                Kmer k = Kmer.createFromCompressed(kb);

                D data = compressor.decompress(bytes);

                consumer.accept(new KmerWithData<D>(k,data));
                return true;
            }
            else
            {
                return false;
            }
        }

        @Override
        public long estimateSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public Spliterator<KmerWithData<D>> trySplit()
        {
            return null;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.IMMUTABLE | Spliterator.ORDERED;
        }

        private byte[] prevkb;
        private Compressor<D> compressor;
        ByteBuffer bytes;
    }
}
