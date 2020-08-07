package KmerFiles;

import Compression.Compressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.OrderedIndexOutput;
import Files.*;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Kmers.KmerWithDataStreamWrapper;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collector;

public class FileCreator<I,O> implements /*Consumer<KmerWithData<I>>,*/ AutoCloseable
{
    public FileCreator(File dbFileTemp, File indexFileTemp,
                       int keyLength, int maxKmerLength, int cacheSize,
                       Compressor<I> inputCompressor, Compressor<O> outputCompressor,
                       Collector<I,?,O> collector) throws IOException
    {
        this.dbFileTemp = dbFileTemp;
        this.indexFileTemp = indexFileTemp;
//        dbFileTemp.deleteOnExit();
//        indexFileTemp.deleteOnExit();

        maxkey = 1;
        for (int i = 0; i < keyLength; i++)
        {
            maxkey *= 4;
        }

        fileCache = new IndexedOutputFileCache(maxkey,cacheSize,new ZippedIndexedOutputFile<>(dbFileTemp,indexFileTemp,5));

        this.keyLength = keyLength;

        this.maxKmerLength = maxKmerLength;

        this.inputCompressor = inputCompressor;

        this.outputCompressor = outputCompressor;

        this.collector = collector;

        this.minK = -1;
        this.maxK = -1;
    }

//    public void accept(KmerWithData<I> kwd)
//    {
//        try
//        {
//            fileCache.add(kwd.getKmer().key(keyLength), kwd.compressedBytes(inputCompressor));
//            KmerWithData<I> rc = new KmerWithData<>(kwd.getKmer().getRC(), kwd.getData());
//            fileCache.add(rc.getKmer().key(keyLength), rc.compressedBytes(inputCompressor));
//        }
//        catch (IOException e)
//        {
//            throw new UncheckedIOException(e);
//        }
//    }

    public void addKmers(KmerWithDataStreamWrapper<I> kmerStream)
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

        kmerStream.stream().forEach(kwd -> {
                        try
                        {
                            fileCache.add(kwd.getKmer().key(keyLength), kwd.compressedBytes(inputCompressor));
                            KmerWithData<I> rc = new KmerWithData<>(kwd.getKmer().getRC(), kwd.getData());
                            fileCache.add(rc.getKmer().key(keyLength), rc.compressedBytes(inputCompressor));
                        }
                        catch (IOException ex)
                        {
                            throw new UncheckedIOException(ex);
                        }
                    }
                );
    }

    public void create(IndexedOutputFile<Integer> out, boolean compress) throws Exception
    {
        fileCache.close();

        IndexedInputFile tempIn = new ZippedIndexedInputFile(dbFileTemp,indexFileTemp);

        LimitedQueueExecutor exec = new LimitedQueueExecutor(7,7);

        OrderedIndexOutput orderedout = new OrderedIndexOutput(out,maxkey);

        for (int i = 0; i < maxkey; i++)
        {
            exec.execute(new MakeAndWriteKey(tempIn,i,orderedout,maxKmerLength, inputCompressor, outputCompressor, collector, compress));
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
        private MakeAndWriteKey(IndexedInputFile in, int index, OrderedIndexOutput out, int maxKmerLength, Compressor<I> inputCompressor,
                                Compressor<O> outputCompressor, Collector<I,A,O> collector, boolean compress)
        {
            this.in = in;
            this.index = index;
            this.out = out;
            this.maxKmerLength = maxKmerLength;
            this.inputCompressor = inputCompressor;
            this.outputCompressor = outputCompressor;
            this.collector = collector;
            this.compress = compress;
        }

        public void run()
        {
            TreeMap<Kmer,A> kmers = new TreeMap<>();
            byte[] data;

            try
            {
//                byte[] indata = in.data(index);
                ByteBuffer indata = ByteBuffer.wrap(in.data(index));
//                int cr = 0;
//                while (cr < indata.length)
                while (indata.hasRemaining())
                {
                    byte len = indata.get();
                    int l = (len - 1) / 4 + 1;
                    byte[] kb = new byte[l+1];
                    kb[0] = len;

                    indata.get(kb,1,l);

                    //Kmer k = Kmer.createFromCompressed(Arrays.copyOfRange(indata, cr, cr + l + 1));
                    Kmer k = Kmer.createFromCompressed(kb);
//                    cr += l + 1;
//                    I d = inputCompressor.decompress(Arrays.copyOfRange(indata, cr, cr + 4));
                    I d = inputCompressor.decompress(indata);
//                    cr += 4;


                    A cm = kmers.get(k);
                    if (cm == null)
                    {
//                        cm = new TreeCountMap<>();
                        cm = collector.supplier().get();
                        kmers.put(k,cm);
                    }
                    //cm.add(d);
                    collector.accumulator().accept(cm,d);
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
                        //                output.write(sb.toString().getBytes("UTF-8"),i);
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

        private Collector<I,A,O> collector;

        private IndexedInputFile in;
        private int index;
        private OrderedIndexOutput out;
        private int maxKmerLength;
        private Compressor<I> inputCompressor;
        private Compressor<O> outputCompressor;
        private boolean compress;
    }

    private int minK;
    private int maxK;

    private File dbFileTemp;
    private File indexFileTemp;

    private Compressor<I> inputCompressor;
    private Compressor<O> outputCompressor;

    private Collector<I,?,O> collector;

    private int maxKmerLength;
    private int keyLength;
    private int maxkey;
    private IndexedOutputFileCache fileCache;
}
