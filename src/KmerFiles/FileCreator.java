package KmerFiles;

import Compression.Compressor;
import Compression.IntCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.OrderedIndexedOutput;
import Concurrent.OrderedLatches;
import Concurrent.OutputProgress;
import Counts.CountDataType;
import DataTypes.DataCollector;
import DataTypes.DataType;
import Exceptions.InconsistentDataException;
import IndexedFiles.*;
import IndexedFiles2.IndexedInputFile2;
import IndexedFiles2.IndexedInputFileSet2;
import IndexedFiles2.IndexedOutputFile2;
import IndexedFiles2.IndexedOutputFileSet2;
import IndexedFiles2.IntegerIndexedOutputFileCache2;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Kmers.KmerStream;
import Kmers.KmerWithDataDataType;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.Collector;

public class FileCreator<I,O> implements AutoCloseable
{
    public FileCreator(File dbFileTemp,
                       int keyLength, int maxKmerLength, int cacheSize,
                       DataCollector<I,O> dataCollector, boolean rc, long maxSize,
                       boolean useExistingTemp) throws IOException
    {
        this.dbFileTemp = dbFileTemp;

        maxkey = 1;
        for (int i = 0; i < keyLength; i++)
        {
            maxkey *= 4;
        }

        this.useExistingTemp = useExistingTemp;
        if (!useExistingTemp)
        {
            fileSet = new IndexedOutputFileSet2<>(f -> new IndexedOutputFile2<>(f, new IntCompressor(), false, 5, maxSize), dbFileTemp);
            fileCache = new IntegerIndexedOutputFileCache2(maxkey, cacheSize, fileSet);
        }

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
                            fileCache.add(kwd.getKmer().key(keyLength), kwdCompressor.compress(kwd));
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

    public void create(IndexedOutputFileSet2<Integer> out, boolean hr) throws Exception
    {
        List<IndexedInputFile2<Integer>> fileList = new ArrayList<>();
        if (!useExistingTemp)
        {
            fileCache.close();
            for (File f: fileSet.getCreated())
            {
                fileList.add(new IndexedInputFile2<>(f, new IntCompressor()));
            }
        }
        else
        {
            if (dbFileTemp.exists())
            {
                fileList.add(new IndexedInputFile2<>(dbFileTemp, new IntCompressor()));
            }
            else
            {
                int i = 1;
                File f = new File(dbFileTemp + "." + i);
                while (f.exists())
                {
                    fileList.add(new IndexedInputFile2<>(f, new IntCompressor()));
                    i++;
                    f = new File(dbFileTemp + "." + i);
                }
            }
        }

        IndexedInputFileSet2<Integer> tempIn = new IndexedInputFileSet2<>(fileList);

        LimitedQueueExecutor<Void> exec = new LimitedQueueExecutor<>();

        OrderedLatches latches = new OrderedLatches(maxkey);

        byte[] meta;
        if (!hr)
        {
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
            exec.submitAndCheck(new MakeAndWriteKey<>(tempIn,i,out, latches, maxK, dataCollector.getDataDataType(),
                    dataCollector.getCollectionDataType(), dataCollector.getCollector(), hr, progress));
        }

        exec.shutdownAndCheck();

        out.close();
        tempIn.close();
        if (dbFileTemp.exists())
        {
            dbFileTemp.delete();
        }
        else
        {
            int i = 1;
            File f = new File(dbFileTemp + "." + i);
            while (f.exists())
            {
                f.delete();
                i++;
                f = new File(dbFileTemp + "." + i);
            }
        }
    }

    public void close() throws Exception
    {
        try
        {
            if (!useExistingTemp)
            {
                fileCache.close();
            }
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
        private MakeAndWriteKey(IndexedInputFileSet2<Integer> in, int index, IndexedOutputFileSet2<Integer> out, OrderedLatches latches, int maxKmerLength,
                                DataType<I> inputCompressor, DataType<O> outputCompressor, Collector<I, A, O> collector, boolean hr, OutputProgress progress)
        {
            this.in = in;
            this.index = index;
            this.out = out;
            this.latches = latches;
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
            byte[][] data;

            try
            {
                DataInputStream indata = new DataInputStream(in.getInputStream(index));

                byte len = (byte) indata.read();
                int c = 0;
                while (len != -1)
                {
                    c++;
                    int l = (len - 1) / 4 + 1;
                    byte[] kb = new byte[l + 1];
                    kb[0] = len;

                    indata.readFully(kb, 1, l);

                    Kmer k = Kmer.createFromCompressed(kb);
                    I d = inputCompressor.decompress(indata);

                    A cm = kmers.get(k);

                    if (cm == null)
                    {
                        cm = collector.supplier().get();
                        kmers.put(k, cm);
                    }
                    collector.accumulator().accept(cm, d);
                    len = (byte) indata.read();
                }
                if (kmers.isEmpty())
                {
                    data = new byte[0][];
                }
                else
                {
                    if (!hr)
                    {
                        data = new byte[kmers.size()*3][];
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

                            data[p] = sb;
                            l++;
                            p++;
                            data[p] = diffkc;
                            l += diffkc.length;
                            p++;
                            byte[] dc = outputCompressor.compress(collector.finisher().apply(e.getValue()));
                            data[p] = dc;
                            l += dc.length;
                            p++;
                        }
                    }
                    else
                    {
                        data = new byte[kmers.size()][];
                        int p = 0;

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
                            data[p] = sb.toString().getBytes("UTF-8");
                            p++;
                            sb.setLength(0);
                        }
                    }
                }
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                throw e;
            }

            try
            {
                latches.wait(index);
                out.setCurrentKey(index);
                for (byte[] d: data)
                {
                    out.write(d);
                }
                latches.done();
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

        private IndexedInputFileSet2<Integer> in;
        private int index;
        private IndexedOutputFileSet2<Integer> out;
        private OrderedLatches latches;
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

    private int keyLength;
    private int maxkey;
    private IntegerIndexedOutputFileCache2 fileCache;
    private IndexedOutputFileSet2<Integer> fileSet;

    private boolean useExistingTemp;
}
