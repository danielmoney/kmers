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
import sun.awt.image.ImageWatched;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
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

        options.addOption(Option.builder("t").hasArg().desc("Accession ID column (this should include version where applicable").build());
        options.addOption(Option.builder("T").hasArg().desc("Taxonomy ID column").build());
        options.addOption(Option.builder("I").hasArg().desc("Number of header lines to ignore").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        if (commands.hasOption("t"))
        {
            LimitedQueueExecutor.setDefaultNumberThreads(Integer.parseInt(commands.getOptionValue('t')));
        }

        File dataFile = new File(commands.getOptionValue('i'));
        File tmpDataFile = new File(commands.getOptionValue('i') + ".tmp");
//        File mapFile = new File(commands.getOptionValue('m'));
        List<File> mapFiles = new LinkedList<>();
        for (String s: commands.getOptionValues('m'))
        {
            mapFiles.add(new File(s));
        }
        File tmpMapFile = new File("map.tmp");
        File outFile = new File(commands.getOptionValue('o'));

        int taxpos = Integer.parseInt(commands.getOptionValue('T',"3")) - 1;
        int idpos = Integer.parseInt(commands.getOptionValue('A',"2")) - 1;
        int headerLines = Integer.parseInt(commands.getOptionValue('I', "1"));

        int keylength = Integer.parseInt(commands.getOptionValue('l',"2"));
        int cachesize = Integer.parseInt(commands.getOptionValue('c',"10000"));

        int z;
        if (commands.hasOption('Z'))
        {
            z = -1;
        }
        else
        {
            z = Integer.parseInt(commands.getOptionValue('z',"5"));
        }

        ExecutorService ex = Executors.newFixedThreadPool(2);

//        makeDataTemp(dataFile, tmpDataFile, keylength, cachesize);
//        makeMapTemp(mapFiles, tmpMapFile, taxpos, idpos, headerLines, keylength,cachesize);

        ex.submit(new MakeDataTemp(dataFile, tmpDataFile, keylength, cachesize));
        ex.submit(new MakeMapTemp(mapFiles, tmpMapFile, taxpos, idpos, headerLines, keylength,cachesize));

        ex.shutdown();
        ex.awaitTermination(14, TimeUnit.DAYS);

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
        IndexedOutputFile<String> out;
        if (z == -1)
        {
            out = new StandardIndexedOutputFile<>(outFile, new StringCompressor(), hr);
        }
        else
        {
            out = new ZippedIndexedOutputFile<>(outFile, new StringCompressor(), hr, z);
        }

        LimitedQueueExecutor<Void> exec = new LimitedQueueExecutor<>();

        for (String index : in2Data.indexes())
        {
            exec.submit(new CreateMapped(in2Data, in2Map, out, index, hr));
        }
        exec.shutdown();
        out.close();


        in2Data.close();
        in2Map.close();

        tmpDataFile.delete();
        tmpMapFile.delete();
    }

    private static class CreateMapped implements Callable<Void>
    {
        public CreateMapped(IndexedInputFile<String> in2Data, IndexedInputFile<String> in2Map,
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

                ByteBuffer input = ByteBuffer.wrap(in2Data.data(index));
                int size = 0;
                while (input.hasRemaining())
                {
                    DataPair<String,Sequence> dp = stringPairDataType.decompress(input);
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
        private IndexedOutputFile<String> out;
        private String index;
    }

    private static class MakeMapTemp implements Callable<Void>
    {
        private MakeMapTemp(List<File> mapFiles, File tmpMapFile, int taxpos, int idpos, int headerLines,
                            int keylength, int cacheSize)
        {
            this.mapFiles = mapFiles;
            this.tmpMapFile = tmpMapFile;
            this.taxpos = taxpos;
            this.idpos = idpos;
            this.headerLines = headerLines;
            this.keylength = keylength;
            this.cacheSize = cacheSize;
        }

        public Void call() throws Exception
        {
            IndexedOutputFileCache<String> tmpMap = new ComparableIndexedOutputFileCache<>(cacheSize,
                    new ZippedIndexedOutputFile<>(tmpMapFile, new StringCompressor(), true, 9));

            for (File mapFile : mapFiles)
            {
                BufferedReader inMap = ZipOrNot.getBufferedReader(mapFile);
                // Well that's annoying recent map file Yucheng gave me doesn't seem to have a header line whereas others do...
                for (int i = 0; i < headerLines; i++)
                {
                    inMap.readLine();
                }
                String line = inMap.readLine();

                while (line != null)
                {
                    String[] parts = line.split("\t");

                    int stop = parts[idpos].indexOf('.');
                    String id = parts[idpos].substring(0, stop);
                    String key = id.substring(id.length() - keylength);

                    tmpMap.add(key, id + "\t" + parts[taxpos] + "\n");

                    line = inMap.readLine();
                }

                inMap.close();
            }
            tmpMap.close();
            return null;
        }

        private List<File> mapFiles;
        private File tmpMapFile;
        private int taxpos;
        private int idpos;
        private int headerLines;
        private int keylength;
        private int cacheSize;
    }

    private static class MakeDataTemp implements Callable<Void>
    {
        private MakeDataTemp(File dataFile, File tmpDataFile, int keylength, int cacheSize) throws Exception
        {
            this.dataFile = dataFile;
            this.tmpDataFile = tmpDataFile;
            this.keylength = keylength;
            this.cacheSize = cacheSize;
        }

        public Void call() throws Exception
        {
            Stream<DataPair<String, Sequence>> sequences = StreamSupport.stream(new FASequenceSpliterator(dataFile), false);

            IndexedOutputFileCache<String> cache = new ComparableIndexedOutputFileCache<>(cacheSize,
                    new ZippedIndexedOutputFile<>(tmpDataFile, new StringCompressor(), false, 5));

            sequences.forEach(s -> writeSeq(cache, s, keylength));

            cache.close();

            return null;
        }

        private File dataFile;
        private File tmpDataFile;
        private int keylength;
        private int cacheSize;
    }

    private static void writeSeq(IndexedOutputFileCache<String> cache, DataPair<String,Sequence> data, int keylength)
    {
        String index = data.getA().substring(data.getA().length()-keylength);
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