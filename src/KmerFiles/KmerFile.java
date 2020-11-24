package KmerFiles;

import Compression.Compressor;
import Compression.IntCompressor;
import DataTypes.MergeableDataType;
import Exceptions.UnexpectedDataTypeException;
import IndexedFiles2.IndexedInputFile2;
import Kmers.Kmer;
import Kmers.KmerUtils;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Zip.ZipOrNot;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KmerFile<D>
{
    public KmerFile(File file, MergeableDataType<D> dataType) throws IOException
    {
        this.file =  new IndexedInputFile2<>(file, new IntCompressor());
        this.dataType = dataType;
        this.meta = getMetaData(file);
        if (!Arrays.equals(meta.dataID,dataType.getID()))
        {
            throw new UnexpectedDataTypeException(file);
        }
    }

    private Stream<KmerWithData<D>> getKmers(int minKey, int maxKey)
    {
        try
        {
            if (file.isHumanReadable())
            {
                return StreamSupport.stream(new UncompressedKmerSpliterator<>(file.getInputStream(minKey,maxKey), dataType), false);
            }
            else
            {
                return StreamSupport.stream(new CompressedKmerSpliterator<>(file.getInputStream(minKey,maxKey), dataType), false);
            }
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    public KmerStream<D> kmers(int key)
    {
        return new KmerStream<>(getKmers(key,key),meta.minLength,meta.maxLength, meta.rc);
    }

    public KmerStream<D> kmers(int startKey, int endKey)
    {
        return new KmerStream<>(getKmers(startKey,endKey),meta.minLength,meta.maxLength, meta.rc);
    }

    public KmerStream<D> allKmers()
    {
        return new KmerStream<>(getKmers(0,file.maxIndex()),meta.minLength,meta.maxLength, meta.rc);
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

    protected IndexedInputFile2<Integer> file;
    private MergeableDataType<D> dataType;
    private MetaData meta;

    public static class MetaData
    {
        public MetaData(int minLength, int maxLength, int keyLength, int[] dataID, boolean rc)
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
        public int[] dataID;
        public boolean rc;
    }

    public class UncompressedKmerSpliterator<D> implements Spliterator<KmerWithData<D>>
    {
        public UncompressedKmerSpliterator(InputStream stream, Compressor<D> compressor)
        {
            in = new BufferedReader(new InputStreamReader(stream));
            this.compressor = compressor;
            prev = null;
        }

        @Override
        public boolean tryAdvance(Consumer<? super KmerWithData<D>> consumer)
        {
            try
            {
                String line = in.readLine();
                if (line != null)
                {
                    String[] parts = line.split("\t");
                    Kmer kmer = Kmer.createUnchecked(prev, parts[0]);
                    D data = compressor.fromString(parts[1]);
                    prev = kmer;
                    consumer.accept(new KmerWithData<>(kmer, data));
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException(ex);
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

        private Kmer prev;
        private Compressor<D> compressor;
        private BufferedReader in;
    }

    public class CompressedKmerSpliterator<D> implements Spliterator<KmerWithData<D>>
    {
        public CompressedKmerSpliterator(InputStream stream, Compressor<D> compressor)
        {
            this.stream = new DataInputStream(stream);
            this.compressor = compressor;
            prevkb = new byte[0];
        }

        @Override
        public boolean tryAdvance(Consumer<? super KmerWithData<D>> consumer)
        {
            try
            {
                byte copy;
                try
                {
                    copy = stream.readByte();
                }
                catch (EOFException ex)
                {
                    return false;
                }

                byte len;
                if (copy == 0)
                {
                    len = stream.readByte();
                }
                else
                {
                    len = prevkb[0];
                }

                int l = (len - 1) / 4 + 1;
                byte[] kb = new byte[l + 1];

                if (copy > 0)
                {
                    System.arraycopy(prevkb, 0, kb, 0, copy);
                    stream.readFully(kb, copy, l - copy + 1);
                }
                else
                {
                    kb[0] = len;
                    stream.readFully(kb, 1, l);
                }

                prevkb = kb;
                Kmer k = Kmer.createFromCompressed(kb);

                D data = compressor.decompress(stream);

                consumer.accept(new KmerWithData<D>(k, data));
                return true;
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException(ex);
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
        private DataInputStream stream;
    }

    public static <D> MetaData getMetaData(File f) throws IOException
    {
        IndexedInputFile2<Integer> file =  new IndexedInputFile2<>(f, new IntCompressor());
        if (file.isHumanReadable())
        {
            BufferedReader in = new BufferedReader(new InputStreamReader(file.getInputStream(-1)));
            return new MetaData(Integer.parseInt(in.readLine()), Integer.parseInt(in.readLine()), Integer.parseInt(in.readLine()),
                    Compressor.getID(in.readLine()), in.readLine().equals("1"));
        }
        else
        {
            DataInputStream in = new DataInputStream(file.getInputStream(-1));
            return new MetaData(in.read(), in.read(), in.read(), Compressor.getID(in), in.read() == 1);
        }
    }

    public static int[] getDataID(File file) throws IOException
    {
        return getMetaData(file).dataID;
    }
}

