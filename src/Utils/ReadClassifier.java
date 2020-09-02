package Utils;

import Compression.IntCompressor;
import Concurrent.LimitedQueueExecutor;
import Concurrent.ListOrderedIndexedOutput;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.DataPair;
import DataTypes.DataPairDataType;
import DataTypes.IntDataType;
import DataTypes.MapDataType;
import IndexedFiles.IndexedInputFile;
import IndexedFiles.ZippedIndexedInputFile;
import IndexedFiles.ZippedIndexedOutputFile;
import Reads.ReadPos;
import Reads.ReadPosDataType;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

public class ReadClassifier
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        DataPairDataType<ReadPos, Map<Integer, TreeCountMap<Integer>>> idt = new DataPairDataType<>(new ReadPosDataType(),
                new MapDataType<>(new IntDataType(), new CountDataType("x","|")), "\t");

        IndexedInputFile<Integer> in = new ZippedIndexedInputFile<>(new File("reads.gz"), new IntCompressor());

        ListOrderedIndexedOutput<Integer> out = new ListOrderedIndexedOutput<>(
                new ZippedIndexedOutputFile<>(new File("classified.gz"), new IntCompressor(),true,5),
                in.indexes());

        LimitedQueueExecutor<Void> ex = new LimitedQueueExecutor<>(1,1);

        for (Integer index: in.indexes())
        {
            ex.submit(new ProcessIndex(in, index, idt, out));
        }

        ex.shutdown();

        out.close();
        in.close();
    }

    private static class ProcessIndex implements Callable<Void>
    {
        private ProcessIndex(IndexedInputFile<Integer> in, int index,
                             DataPairDataType<ReadPos, Map<Integer, TreeCountMap<Integer>>> idt,
                             ListOrderedIndexedOutput<Integer> out)
        {
            this.in = in;
            this.index = index;
            this.idt = idt;
            this.out = out;
        }

        public Void call()
        {
            Iterator<String> inData = in.lines(index).iterator();

            DataPair<ReadPos, Map<Integer, TreeCountMap<Integer>>> data;
            int curRead = -1;
            List<Set<Integer>> taxids = new LinkedList<>();
            List<DataPair<Integer,Integer>> called = new LinkedList<>();
            if (inData.hasNext())
            {
                data = idt.fromString(inData.next());
                curRead = data.getA().getRead();
                taxids.add(data.getB().get(0).keySet());
            }

            while (inData.hasNext())
            {
                 data = idt.fromString(inData.next());
                 if (data.getA().getRead() != curRead)
                 {
                     Integer call = processRead(taxids);
                     if (call != null)
                     {
                         called.add(new DataPair<>(curRead,call));
                     }
                     curRead = data.getA().getRead();
                     taxids = new LinkedList<>();
                 }
                 taxids.add(data.getB().get(0).keySet());
            }

            Integer call = processRead(taxids);
            if (call != null)
            {
                called.add(new DataPair<>(curRead,call));
            }

            byte[][] temp = new byte[called.size()][];
            int c = 0;
            int s = 0;
            DataPairDataType<Integer,Integer> odt = new DataPairDataType<>(new IntDataType(), new IntDataType(), "\t");
            for (DataPair<Integer,Integer> cc: called)
            {
                temp[c] = (odt.toString(cc) + "\n").getBytes();
                s += temp[c].length;
                c++;
            }
            ByteBuffer bb = ByteBuffer.allocate(s);
            for (byte[] b: temp)
            {
                bb.put(b);
            }

            try
            {
                out.write(bb.array(), index);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            return null;
        }

        public Integer processRead(List<Set<Integer>> taxids)
        {
            int call = -1;

            for (Set<Integer> tids : taxids)
            {
                if (tids.size() == 1)
                {
                    int cur = tids.iterator().next();;
                    if (call == -1)
                    {
                        call = cur;
                    }
                    else
                    {
                        if (cur != call)
                        {
                            call = -2;
                        }
                    }
                }
            }

            if (call >= 0)
            {
                for (Set<Integer> tids: taxids)
                {
                    if (!tids.contains(call))
                    {
                        call = -2;
                    }
                }
            }

            if (call >= 0)
            {
                return call;
            }
            else
            {
                return null;
            }
        }

        private IndexedInputFile<Integer> in;
        private int index;
        private DataPairDataType<ReadPos, Map<Integer, TreeCountMap<Integer>>> idt;
        private ListOrderedIndexedOutput<Integer> out;
    }
}
