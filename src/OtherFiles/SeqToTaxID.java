package OtherFiles;

import Compression.StringCompressor;
import Concurrent.LimitedQueueExecutor;
import DataTypes.DataPair;
import DataTypes.DataPairDataType;
import DataTypes.IntDataType;
import DataTypes.StringDataType;
import Exceptions.InvalidBaseException;
import IndexedFiles.*;
import Kmers.*;
import Zip.BlockedZipOutputFile;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SeqToTaxID
{
    public static void main(String[] args) throws Exception
    {
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").required().hasArg().desc("Input file").build());
        options.addOption(Option.builder("m").required().hasArg().desc("Map File").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        OptionGroup zipoptions = new OptionGroup();
        zipoptions.addOption(Option.builder("z").hasArg().desc("Zip compression level").build());
        zipoptions.addOption(Option.builder("Z").hasArg().desc("Unzipped output").build());
        options.addOptionGroup(zipoptions);

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        if (commands.hasOption("t"))
        {
            LimitedQueueExecutor.setDefaultNumberThreads(Integer.parseInt(commands.getOptionValue('t')));
        }

        File dataFile = new File(commands.getOptionValue('i'));
        File tmpDataFile = new File(commands.getOptionValue('i') + ".tmp");
        File mapFile = new File(commands.getOptionValue('m'));
        File tmpMapFile = new File(commands.getOptionValue('m') + ".tmp");
        File outFile = new File(commands.getOptionValue('o'));
        int taxpos = 2;
        int idpos = 1;

        int z;
        if (commands.hasOption('Z'))
        {
            z = -1;
        }
        else
        {
            z = Integer.parseInt(commands.getOptionValue('z',"5"));
        }


        makeDataTemp(dataFile, tmpDataFile);
        makeMapTemp(mapFile, tmpMapFile, taxpos, idpos);
        createMapped(tmpDataFile,tmpMapFile,outFile,z,commands.hasOption('h'));


        System.out.println(sdf.format(new Date()));
    }


    public static void createMapped(File tmpDataFile, File tmpMapFile, File outFile, int z, boolean hr) throws IOException, InterruptedException
    {
        IndexedInputFile<String> in2Data = new ZippedIndexedInputFile<>(tmpDataFile, new StringCompressor());
        IndexedInputFile<String> in2Map = new ZippedIndexedInputFile<>(tmpMapFile, new StringCompressor());


//        if (z != -1)
//        {
//            BlockedZipOutputFile out = new BlockedZipOutputFile(outFile, 5);
            IndexedOutputFile<String> out = new ZippedIndexedOutputFile<>(outFile, new StringCompressor(), hr, z);

            LimitedQueueExecutor<Void> exec = new LimitedQueueExecutor<>();

            for (String index : in2Data.indexes())
            {
                exec.submit(new CreateMapped(in2Data, in2Map, out, index, hr));
            }
            exec.shutdown();
            out.close();
//        }
//        else
//        {
//            //No point in doing multithreaded stuff if not zipping as limiting factor is likely to be IO.
//            PrintWriter out = new PrintWriter(new GZIPOutputStream(new BufferedOutputStream(
//                new FileOutputStream(outFile))));
//            for (String index : in2Data.indexes())
//            {
//                Map<String,String> map = new HashMap<>();
//                in2Map.lines(index).forEach(l -> {
//                    String[] parts = l.split("\t");
//                    map.put(parts[0], parts[1]);
//                });
//
//                in2Data.lines(index).forEach(l -> {
//                    String[] parts = l.split("\t",2);
//                    out.println(map.get(parts[0]) + "\t" + parts[1]);
//                });
//            }
//            out.close();
//        }

        in2Data.close();
        in2Map.close();

//        tmpDataFile.delete();
        tmpMapFile.delete();
    }

    private static class CreateMapped implements Callable<Void>
    {
        public CreateMapped(IndexedInputFile<String> in2Data, IndexedInputFile<String> in2Map,
//                            BlockedZipOutputFile out, String index)
                IndexedOutputFile<String> out, String index, boolean hr)
        {
            this.in2Data = in2Data;
            this.in2Map = in2Map;
            this.out = out;
            this.index = index;
            this.hr = hr;
        }

        public Void call()
        {
            try
            {
                Map<String, Integer> map = new HashMap<>();
                in2Map.lines(index).forEach(l -> {
                    String[] parts = l.split("\t");
                    map.put(parts[0], Integer.parseInt(parts[1]));
                });

                LinkedList<byte[]> bytes = new LinkedList<>();

//                int size = in2Data.lines(index).mapToInt(l -> {
//                    String[] parts = l.split("\t", 2);
//                    byte[] b = (map.get(parts[0]) + "\t" + parts[1] + "\n").getBytes();
//                    bytes.add(b);
//                    return b.length;
//                }).sum();

                ByteBuffer input = ByteBuffer.wrap(in2Data.data(index));
                int size = 0;
                while (input.hasRemaining())
                {
                    DataPair<String,Sequence> dp = stringPairDataType.decompress(input);
                    //byte[] b = (map.get(dp.getA()) + "\t" + dp.getB().toString() + "\n").getBytes();
                    DataPair<Integer,Sequence> newdp = new DataPair<>(map.get(dp.getA()),dp.getB());
                    byte[] b;
                    if (hr)
                    {
                        b = (integerPairDataType.toString(newdp) + "\n").getBytes();
                    }
                    else
                    {
                        b = integerPairDataType.compress(newdp);
                    }
                    bytes.add(b);
                    size += b.length;
                }

                ByteBuffer bb = ByteBuffer.allocate(size);
                for (byte[] b : bytes)
                {
                    bb.put(b);
                }
                //out.writeBlock(bb.array());
                out.write(bb.array(),index);
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException(ex);
            }

            return null;
        }

        private boolean hr;
        private IndexedInputFile<String> in2Data;
        private IndexedInputFile<String> in2Map;
//        private BlockedZipOutputFile out;
        private IndexedOutputFile<String> out;
        private String index;
    }

    public static void makeMapTemp(File mapFile, File tmpMapFile, int taxpos, int idpos) throws Exception
    {
        BufferedReader inMap = ZipOrNot.getBufferedReader(mapFile);
        IndexedOutputFileCache<String> tmpMap = new ComparableIndexedOutputFileCache<>(1000,
                new ZippedIndexedOutputFile<>(tmpMapFile, new StringCompressor(), true, 9));

        // Well that's annoying recent map file Yucheng gave me doesn't seem to have a header line whereas others do...
//        inMap.readLine();
        String line = inMap.readLine();

        while (line != null)
        {
            String[] parts = line.split("\t");

            int stop = parts[idpos].indexOf('.');
            String id = parts[idpos].substring(0, stop);
            String key = id.substring(parts[idpos].length()-4);

            tmpMap.add(key, id + "\t" + parts[taxpos] + "\n");

            line = inMap.readLine();
        }

        inMap.close();
        tmpMap.close();
    }

    public static void makeDataTemp(File dataFile, File tmpDataFile) throws Exception
    {
        Stream<DataPair<String,Sequence>> sequences = StreamSupport.stream(new FASequenceSpliterator(dataFile),false);

        IndexedOutputFileCache<String> cache = new ComparableIndexedOutputFileCache<>(1000,
                new ZippedIndexedOutputFile<>(tmpDataFile,new StringCompressor(),false,5));

        sequences.forEach(s -> writeSeq(cache, s));

        cache.close();
    }

    private static void writeSeq(IndexedOutputFileCache<String> cache, DataPair<String,Sequence> data)
    {
        String index = data.getA().substring(data.getA().length()-2);
        try
        {
            cache.add(index, stringPairDataType.compress(data));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static DataPairDataType<String,Sequence> stringPairDataType = new DataPairDataType<>(new StringDataType(), new SequenceDataType());
    private static DataPairDataType<Integer,Sequence> integerPairDataType = new DataPairDataType<>(new IntDataType(), new SequenceDataType());

    private static class FASequenceSpliterator implements Spliterator<DataPair<String,Sequence>>
    {
        private FASequenceSpliterator(File dataFile) throws IOException
        {
            inData = ZipOrNot.getDataInputStream(dataFile);
            id = "";
            curb = inData.readByte();
            if (curb == '>')
            {
                state = InputState.ID;
            }
            else
            {
                state = InputState.RESTID;
            }
        }

        public boolean tryAdvance(Consumer<? super DataPair<String, Sequence>> consumer)
        {
            boolean newseq = false;
            ByteArrayOutputStream seq = new ByteArrayOutputStream();
            ByteArrayOutputStream idbuilder = new ByteArrayOutputStream();
            try
            {
                while (!newseq)
                {
                    curb = inData.readByte();

                    switch (state)
                    {
                        case ID:
                            switch (curb)
                            {
                                case '.':
                                case ' ':
                                case '\t':
                                    state = InputState.RESTID;
                                    id = new String(idbuilder.toByteArray());
                                    idbuilder.reset();
                                    break;
                                case '\n':
                                    state = InputState.SEQ;
                                    id = new String(idbuilder.toByteArray());
                                    idbuilder.reset();
                                    break;
                                default:
                                    idbuilder.write(curb);
                            }
                            break;
                        case RESTID:
                            switch (curb)
                            {
                                case '\n':
                                    state = InputState.SEQ;
                                    break;
                            }
                            break;
                        case SEQ:
                            switch (curb)
                            {
                                case '\n':
                                    break;
                                case '>':
                                    if (seq.size() > 0)
                                    {
                                        newseq = true;
                                    }
                                    state = InputState.ID;
                                    break;
                                default:
                                    try
                                    {
                                        seq.write(Base.fromCharacterByte(curb).pos());
                                    }
                                    catch (InvalidBaseException e)
                                    {
                                        if (seq.size() > 0)
                                        {
                                            newseq = true;
                                        }
                                    }
                            }
                            break;
                    }
                }

            }
            catch (EOFException ex)
            {
                if ((state == InputState.SEQ) && (seq.size() > 0))
                {
                    newseq = true;
                }
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException(ex);
            }


            if (newseq)
            {
                DataPair<String, Sequence> ret = new DataPair<>(id, Sequence.createUnchecked(seq.toByteArray()));
                consumer.accept(ret);
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
        public Spliterator<DataPair<String,Sequence>> trySplit()
        {
            return null;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.IMMUTABLE | Spliterator.ORDERED;
        }

        private InputState state;
        private byte curb;
        private String id;
        private DataInputStream inData;
    }

    private static enum InputState
    {
        ID,
        RESTID,
        SEQ,
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}