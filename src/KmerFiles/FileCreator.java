package KmerFiles;

import Compression.Compressor;
import Compression.IntCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.OrderedIndexedOutput;
import Concurrent.OutputProgress;
import DataTypes.DataCollector;
import DataTypes.DataType;
import Exceptions.InconsistentDataException;
import IndexedFiles.*;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Kmers.KmerWithDataDataType;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collector;

public class FileCreator<I,O> implements AutoCloseable
{
    public FileCreator(File dbFileTemp,
                       int keyLength, int maxKmerLength, int cacheSize,
                       DataCollector<I,O> dataCollector, boolean rc, long maxSize) throws IOException
    {
        this.dbFileTemp = dbFileTemp;
//        dbFileTemp.deleteOnExit();

        maxkey = 1;
        for (int i = 0; i < keyLength; i++)
        {
            maxkey *= 4;
        }

        fileSet = new IndexedOutputFileSet<>(f -> new ZippedIndexedOutputFile<>(f,new IntCompressor(),false,5,maxSize), dbFileTemp);
        fileCache = new IntegerIndexedOutputFileCache(maxkey,cacheSize,fileSet);

//        fileCache = new IntegerIndexedOutputFileCache(maxkey,cacheSize,new ZippedIndexedOutputFile<>(dbFileTemp,new IntCompressor(),false,5));
//        fileCache = new IndexedOutputFileCache2<>(cacheSize,new ZippedIndexedOutputFile<>(dbFileTemp,new IntCompressor(),false,5));

        this.keyLength = keyLength;

        this.dataCollector = dataCollector;

        kwdCompressor = new KmerWithDataDataType<>(dataCollector.getDataDataType());

        this.minK = -1;
        this.maxK = -1;

        this.rc = rc;
    }

    public void addKmers(KmerStream<I> kmerStream) throws InconsistentDataException
    {
        if (minK == -1)
        {
            minK = kmerStream.getMinLength();
            maxK = kmerStream.getMaxLength();
        }
        if ( (minK != kmerStream.getMinLength()) || (maxK != kmerStream.getMaxLength()) )
        {
            throw new InconsistentDataException("New stream does not have the same min or max kmer length as a previous stream");
        }

        if (rc)
        {
            kmerStream.forEach(kwd -> {
                        try
                        {
//                            fileCache.add(kwd.getKmer().key(keyLength), kwd.compressedBytes(dataType.getDataCompressor()));
                            fileCache.add(kwd.getKmer().key(keyLength), kwdCompressor.compress(kwd));
//                            KmerWithData<I> rc = new KmerWithData<>(kwd.getKmer().getRC(), kwd.getData());
//                            //Check for plaindromes
//                            if (!kwd.getKmer().equals(rc.getKmer()))
//                            {
////                                fileCache.add(rc.getKmer().key(keyLength), rc.compressedBytes(dataType.getDataCompressor()));
//                                fileCache.add(rc.getKmer().key(keyLength), kwdCompressor.compress(rc));
//                            }
                            if (!kwd.getKmer().isOwnRC())
                            {
                                kwd.inplaceRC();
                                fileCache.add(kwd.getKmer().key(keyLength), kwdCompressor.compress(kwd));
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

    public void create(IndexedOutputFileSet<Integer> out, boolean hr) throws Exception
    {
        fileCache.close();

        List<IndexedInputFile<Integer>> fileList = new ArrayList<>();
        for (File f: fileSet.getCreated())
        {
            fileList.add(new ZippedIndexedInputFile<>(f, new IntCompressor()));
        }

        //IndexedInputFile<Integer> tempIn = new ZippedIndexedInputFile<>(dbFileTemp, new IntCompressor());
        IndexedInputFileSet<Integer> tempIn = new IndexedInputFileSet<>(fileList);

        LimitedQueueExecutor<Void> exec = new LimitedQueueExecutor<>();

        OrderedIndexedOutput orderedout = new OrderedIndexedOutput(out,maxkey);

        byte[] meta;
        if (!hr)
        {
//            ByteBuffer bb = ByteBuffer.allocate(8);
//            bb.put((byte) minK);
//            bb.put((byte) maxK);
//            bb.put((byte) keyLength);
//            //bb.putInt(dataCollector.getCollectionDataType().getID());
//            bb.put(Compressor.getByteID(dataCollector.getCollectionDataType().getID()));
//            bb.put(rc ? (byte) 1 : (byte) 0);
//            meta = bb.array();

            byte[] id = Compressor.getByteID(dataCollector.getCollectionDataType().getID());
            ByteBuffer bb = ByteBuffer.allocate(4 + id.length);
            bb.put((byte) minK);
            bb.put((byte) maxK);
            bb.put((byte) keyLength);
            bb.put(id);
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
//            sb.append(dataCollector.getCollectionDataType().getID());
            sb.append(Compressor.getStringID(dataCollector.getCollectionDataType().getID()));
            sb.append("\n");
            sb.append(rc ? "1" : "0");
            sb.append("\n");
            meta = sb.toString().getBytes();
        }
        out.writeAll(meta,-1);

        OutputProgress progress = new OutputProgress("%4d/" + maxkey + " output indexes completed.");

        for (int i = 0; i < maxkey; i++)
        {
            //exec.submit(new MakeAndWriteKey<>(tempIn,i,orderedout, maxKmerLength, dataCollector.getDataDataType(),
            exec.submit(new MakeAndWriteKey<>(tempIn,i,orderedout, maxK, dataCollector.getDataDataType(),
                    dataCollector.getCollectionDataType(), dataCollector.getCollector(), hr, progress));
        }

        exec.shutdown();

        out.close();
        tempIn.close();
//        dbFileTemp.delete();
        for (File f: fileSet.getCreated())
        {
            f.delete();
        }
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
        while ((b1[first] == b2[first]) && (first < b1.length) && (first < b2.length))
        {
            first ++;
        }
        return first;
    }

    private static class MakeAndWriteKey<I,O,A> implements Callable<Void>
    {
        //private MakeAndWriteKey(IndexedInputFile<Integer> in, int index, OrderedIndexedOutput out, int maxKmerLength, DataType<I> inputCompressor,
        private MakeAndWriteKey(IndexedInputFileSet<Integer> in, int index, OrderedIndexedOutput out, int maxKmerLength, DataType<I> inputCompressor,
                                DataType<O> outputCompressor, Collector<I, A, O> collector, boolean hr, OutputProgress progress)
        {
            this.in = in;
            this.index = index;
            this.out = out;
            this.maxKmerLength = maxKmerLength;
            this.inputCompressor = inputCompressor;
            this.outputCompressor = outputCompressor;
            this.collector = collector;
            this.hr = hr;
            this.progress = progress;
        }

        public Void call()
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
                    if (!hr)
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
                // In the normal course of things we shouldn't get here so something unusal has happened
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            progress.next();

            return null;
        }


        private Collector<I, A, O> collector;

        private IndexedInputFileSet<Integer> in;
        private int index;
        private OrderedIndexedOutput out;
        private int maxKmerLength;
        private DataType<I> inputCompressor;
        private DataType<O> outputCompressor;
        private boolean hr;
        private OutputProgress progress;
    }

    private boolean rc;

    private int minK;
    private int maxK;

    private File dbFileTemp;

    private DataCollector<I,O> dataCollector;
    private KmerWithDataDataType<I> kwdCompressor;

//    private int maxKmerLength;
    private int keyLength;
    private int maxkey;
    private IndexedOutputFileCache<Integer> fileCache;
    private IndexedOutputFileSet<Integer> fileSet;
}
