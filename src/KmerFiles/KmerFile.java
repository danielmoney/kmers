package KmerFiles;

import Compression.Compressor;
import Compression.IntCompressor;
import DataTypes.MergeableDataType;
import IndexedFiles.IndexedInputFile;
import IndexedFiles.StandardIndexedInputFile;
import IndexedFiles.ZippedIndexedInputFile;
import Kmers.Kmer;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Zip.ZipOrNot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KmerFile<D>
{
    public KmerFile(File file, MergeableDataType<D> dataType) throws IOException
    {
        this.file = getIndexedInputFile(file);
        this.dataType = dataType;
        this.meta = getMetaData(file);
        if (meta.dataID != dataType.getID())
        {
            //throw an error
        }
    }

    private Stream<KmerWithData<D>> getKmers(int key)
    {
        if (file.isHumanReadable())
        {
            return getUncompressedKmers(key);
        }
        else
        {
            return StreamSupport.stream(new CompressedKmerSpliterator<>(file.data(key), dataType), false);
        }
    }

    public KmerStream<D> kmers(int key)
    {
        return new KmerStream<>(getKmers(key),meta.minLength,meta.maxLength, meta.rc);
    }

    public KmerStream<D> kmers(int startKey, int endKey)
    {
        return new KmerStream<>(IntStream.range(startKey,endKey).filter(i -> file.hasIndex(i)).mapToObj(i ->
                getKmers(i)).flatMap(s -> s), meta.minLength, meta.maxLength, meta.rc);
    }

    public KmerStream<D> allKmers()
    {
        return kmers(0, file.maxIndex());
    }



    public KmerStream<D> restrictedKmers(int key, int minLength, int maxLength)
    {
        return KmerUtils.restrictedStream(kmers(key),minLength,maxLength,dataType);
    }

    public KmerStream<D> restrictedKmers(int minKey, int maxKey, int minLength, int maxLength)
    {
        return KmerUtils.restrictedStream(kmers(minKey, maxKey),minLength,maxLength,dataType);
    }

    public KmerStream<D> allRestrictedKmers(int minLength, int maxLength)
    {
        return KmerUtils.restrictedStream(allKmers(),minLength,maxLength,dataType);
    }

    public int getKeyLength()
    {
        return meta.keyLength;
    }

    public int getMinLength()
    {
        return meta.minLength;
    }

    public int getMaxLength()
    {
        return meta.maxLength;
    }

    public boolean getRC()
    {
        return meta.rc;
    }

    public MergeableDataType<D> getDataType()
    {
        return dataType;
    }

    private Stream<KmerWithData<D>> getUncompressedKmers(int key)
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
                        D data = dataType.fromString(parts[1]);
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

    protected IndexedInputFile<Integer> file;
    private MergeableDataType<D> dataType;
    private MetaData meta;

    private static class MetaData
    {
        public MetaData(int minLength, int maxLength, int keyLength, int dataID, boolean rc)
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
            this.keyLength = keyLength;
            this.dataID = dataID;
            this.rc = rc;
        }

        public int minLength;
        public int maxLength;
        public int keyLength;
        public int dataID;
        public boolean rc;
    }

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

    private static IndexedInputFile<Integer> getIndexedInputFile(File f) throws IOException
    {
        if (ZipOrNot.isGZipped(f))
        {
            return new ZippedIndexedInputFile<>(f, new IntCompressor());
        }
        else
        {
            return new StandardIndexedInputFile<>(f, new IntCompressor());
        }
    }

    private static <D> MetaData getMetaData(File f) throws IOException
    {
        IndexedInputFile<Integer> file = getIndexedInputFile(f);
        byte[] metadata = file.data(-1);
        if (file.isHumanReadable())
        {
            String meta = new String(file.data(-1));
            String[] parts = meta.split("\n");
            return new MetaData(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
                    Integer.parseInt(parts[2]), Integer.parseInt(parts[3]), parts[4].equals("1"));
        }
        else
        {
            ByteBuffer meta = ByteBuffer.wrap(file.data(-1));
            return new MetaData(meta.get(), meta.get(), meta.get(), meta.getInt(), meta.get() == (byte) 1);
        }
    }

    public static int getDataID(File file) throws IOException
    {
        return getMetaData(file).dataID;
    }
}

