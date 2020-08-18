package KmerFiles;

import Compression.Compressor;
import Compression.IntCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.OrderedIndexOutput;
import DataTypes.DataCollector;
import DataTypes.DataType;
import IndexedFiles.*;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Kmers.KmerWithDataDatatType;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public class FileCreator<I,O> implements AutoCloseable
{
    public FileCreator(File dbFileTemp,
                       int keyLength, int maxKmerLength, int cacheSize,
                       DataCollector<I,O> dataCollector, boolean rc) throws IOException
    {
        this.dbFileTemp = dbFileTemp;
        dbFileTemp.deleteOnExit();

        maxkey = 1;
        for (int i = 0; i < keyLength; i++)
        {
            maxkey *= 4;
        }

        fileCache = new IndexedOutputFileCache(maxkey,cacheSize,new ZippedIndexedOutputFile<>(dbFileTemp,new IntCompressor(),false,5));

        this.keyLength = keyLength;

        this.dataCollector = dataCollector;

        kwdCompressor = new KmerWithDataDatatType<>(dataCollector.getDataDataType());

        this.minK = -1;
        this.maxK = -1;

        this.rc = rc;
    }

    public void addKmers(KmerStream<I> kmerStream)
    {
        // Need some checking here!!
        if (minK == -1)
        {
            minK = kmerStream.getMinLength();
            maxK = kmerStream.getMaxLength();
        }
        if ( (minK != kmerStream.getMinLength()) || (maxK != kmerStream.getMaxLength()) )
        {
            // Throw some exception!!
        }

        if (rc)
        {
            kmerStream.forEach(kwd -> {
                        try
                        {
//                            fileCache.add(kwd.getKmer().key(keyLength), kwd.compressedBytes(dataType.getDataCompressor()));
                            fileCache.add(kwd.getKmer().key(keyLength), kwdCompressor.compress(kwd));
                            KmerWithData<I> rc = new KmerWithData<>(kwd.getKmer().getRC(), kwd.getData());
                            //Check for plaindromes
                            if (!kwd.getKmer().equals(rc.getKmer()))
                            {
//                                fileCache.add(rc.getKmer().key(keyLength), rc.compressedBytes(dataType.getDataCompressor()));
                                fileCache.add(rc.getKmer().key(keyLength), kwdCompressor.compress(rc));
                            }
                        }
                        catch (IOException ex)
                        {
                            throw new UncheckedIOException(ex);
                        }
                    }
            );
        }
        else
        {
            kmerStream.forEach(kwd -> {
                        try
                        {
//                            fileCache.add(kwd.getKmer().key(keyLength), kwd.compressedBytes(dataType.getDataCompressor()));
                            fileCache.add(kwd.getKmer().key(keyLength), kwdCompressor.compress(kwd));
                        }
                        catch (IOException ex)
                        {
                            throw new UncheckedIOException(ex);
                        }
                    }
            );
        }
    }

    public void create(IndexedOutputFile<Integer> out, boolean hr) throws Exception
    {
        fileCache.close();

        IndexedInputFile<Integer> tempIn = new ZippedIndexedInputFile<>(dbFileTemp, new IntCompressor());

        LimitedQueueExecutor exec = new LimitedQueueExecutor(7,7);

        OrderedIndexOutput orderedout = new OrderedIndexOutput(out,maxkey);

        byte[] meta;
        if (hr)
        {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.put((byte) minK);
            bb.put((byte) maxK);
            bb.put((byte) keyLength);
            bb.putInt(dataCollector.getCollectionDataType().getID());
            bb.put(rc ? (byte) 1 : (byte) 0);
            meta = bb.array();
        }
        else
        {
            StringBuffer sb = new StringBuffer();
            sb.append(minK);
            sb.append("\n");
            sb.append(maxK);
            sb.append("\n");
            sb.append(keyLength);
            sb.append("\n");
            sb.append(dataCollector.getCollectionDataType().getID());
            sb.append("\n");
            sb.append(rc ? "1" : "0");
            sb.append("\n");
            meta = sb.toString().getBytes();
        }
        out.write(meta,-1);

        for (int i = 0; i < maxkey; i++)
        {
            exec.execute(new MakeAndWriteKey<>(tempIn,i,orderedout, maxKmerLength, dataCollector.getDataDataType(),
                    dataCollector.getCollectionDataType(), dataCollector.getCollector(), hr));
        }

        exec.shutdown();

        out.close();
        tempIn.close();
    }

    public void close() throws Exception
    {
        try
        {
            fileCache.close();
        }
        catch (IOException e)
        {
            // Not sure what to do here!
        }
    }

    private static String diff(String s1, String s2)
    {
        int first = 0;
        while (s1.charAt(first) == s2.charAt(first))
        {
            first ++;
        }
        return s2.substring(first);
    }

    private static int shared(byte[] b1, byte[] b2)
    {
        int first = 0;
        while (b1[first] == b2[first])
        {
            first ++;
        }
        return first;
    }

    private static class MakeAndWriteKey<I,O,A> implements Runnable
    {
        private MakeAndWriteKey(IndexedInputFile in, int index, OrderedIndexOutput out, int maxKmerLength, DataType<I> inputCompressor,
                                DataType<O> outputCompressor, Collector<I, A, O> collector, boolean hr)
        {
            this.in = in;
            this.index = index;
            this.out = out;
            this.maxKmerLength = maxKmerLength;
            this.inputCompressor = inputCompressor;
            this.outputCompressor = outputCompressor;
            this.collector = collector;
            this.compress = hr;
        }

        public void run()
        {
            TreeMap<Kmer, A> kmers = new TreeMap<>();
            byte[] data;

            try
            {
                ByteBuffer indata = ByteBuffer.wrap(in.data(index));
                while (indata.hasRemaining())
                {
                    byte len = indata.get();
                    int l = (len - 1) / 4 + 1;
                    byte[] kb = new byte[l + 1];
                    kb[0] = len;

                    indata.get(kb, 1, l);

                    Kmer k = Kmer.createFromCompressed(kb);
                    I d = inputCompressor.decompress(indata);


                    A cm = kmers.get(k);
                    if (cm == null)
                    {
                        cm = collector.supplier().get();
                        kmers.put(k, cm);
                    }
                    collector.accumulator().accept(cm, d);
                }

                if (kmers.isEmpty())
                {
                    data = new byte[0];
                }
                else
                {
                    if (compress)
                    {
                        byte[][] bytes = new byte[kmers.size() * 3][];
                        int p = 0;
                        int l = 0;
                        byte[] prev = new byte[1];

                        for (Map.Entry<Kmer, A> e : kmers.entrySet())
                        {
                            byte[] kc = e.getKey().compressedBytes();
                            int s = shared(prev, kc);
                            prev = kc;

                            byte[] sb = new byte[1];
                            sb[0] = (byte) s;

                            byte[] diffkc;
                            if (s == 0)
                            {
                                diffkc = kc;
                            }
                            else
                            {
                                diffkc = new byte[kc.length - s];
                                System.arraycopy(kc, s, diffkc, 0, diffkc.length);
                            }

                            bytes[p] = sb;
                            l++;
                            p++;
                            bytes[p] = diffkc;
                            l += diffkc.length;
                            p++;
                            byte[] dc = outputCompressor.compress(collector.finisher().apply(e.getValue()));
                            bytes[p] = dc;
                            l += dc.length;
                            p++;
                        }

                        data = new byte[l];
                        int c = 0;
                        for (byte[] b : bytes)
                        {
                            System.arraycopy(b, 0, data, c, b.length);
                            c += b.length;
                        }
                    }
                    else
                    {
                        char[] chars = new char[kmers.firstKey().length()];
                        Arrays.fill(chars, ' ');
                        String last = new String(chars);
                        StringBuilder sb = new StringBuilder();
                        for (Map.Entry<Kmer, A> e : kmers.entrySet())
                        {
                            String next = e.getKey().toDBString(maxKmerLength);
                            sb.append(diff(last, next));
                            sb.append("\t");
                            sb.append(outputCompressor.toString(collector.finisher().apply(e.getValue())));
                            sb.append("\n");
                            last = next;
                        }
                        data = sb.toString().getBytes("UTF-8");
                    }
                }
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            try
            {
                out.write(data, index);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }


        private Collector<I, A, O> collector;

        private IndexedInputFile in;
        private int index;
        private OrderedIndexOutput out;
        private int maxKmerLength;
        private DataType<I> inputCompressor;
        private DataType<O> outputCompressor;
        private boolean compress;
    }

    private boolean rc;

    private int minK;
    private int maxK;

    private File dbFileTemp;

    private DataCollector<I,O> dataCollector;
    private KmerWithDataDatatType<I> kwdCompressor;

    private int maxKmerLength;
    private int keyLength;
    private int maxkey;
    private IndexedOutputFileCache fileCache;
}
