package Database;

import Concurrent.LimitedQueueExecutor;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.DataPair;
import DataTypes.DataPairDataType;
import DataTypes.ResultsDataType;
import DataTypes.SetDataType;
import Exceptions.InconsistentDataException;
import KmerFiles.KmerFile;
import Kmers.KmerDiff;
import Kmers.KmerDiffDataType;
import Kmers.KmerStream;
import Kmers.KmerWithDataDataType;
import Reads.ReadPos;
import Reads.ReadPosDataType;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class Matcher
{
    public static void main(String[] args) throws ParseException, IOException, InconsistentDataException
    {
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").required().hasArg().desc("Input (reads) file").build());
        options.addOption(Option.builder("d").required().hasArg().desc("Database file (may be more than one)").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        options.addOption(Option.builder("n").hasArg().desc("Max difference between search and match kmers (default: 0)").build());
        options.addOption(Option.builder("j").desc("Return just the best matches rather than all matches").build());

        options.addOption(Option.builder("K").hasArg().desc("Max kmer length").build());
        options.addOption(Option.builder("k").hasArg().desc("Min kmer length").build());

        OptionGroup zipoptions = new OptionGroup();
        zipoptions.addOption(Option.builder("z").hasArg().desc("Zip compression level").build());
        zipoptions.addOption(Option.builder("Z").hasArg().desc("Unzipped output").build());
        options.addOptionGroup(zipoptions);

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        int z = Integer.parseInt(commands.getOptionValue('z', "5"));

        List<KmerFile<TreeCountMap<Integer>>> dbfiles = new LinkedList<>();
        for (String s: commands.getOptionValues('d'))
        {
            File f = new File(s);
            if (f.exists())
            {
                dbfiles.add(new KmerFile<>(f, new CountDataType()));
            }
            else
            {
                int i = 1;
                f = new File(s + "." + i);
                while (f.exists())
                {
                    dbfiles.add(new KmerFile<>(f, new CountDataType()));
                    i ++;
                    f = new File(s + "." + i);
                }
            }
        }
        DB<TreeCountMap<Integer>> db = new DB<>(dbfiles);

        if (commands.hasOption("t"))
        {
//            LimitedQueueExecutor.setDefaultNumberThreads(Integer.parseInt(commands.getOptionValue('t')));
            db.setThreads(Integer.parseInt(commands.getOptionValue('t')));
        }


        List<File> searchfiles = new LinkedList<>();
        String s = commands.getOptionValue('i');
        File f = new File(s);
        if (f.exists())
        {
            searchfiles.add(f);
        }
        else
        {
            int i = 1;
            f = new File(s + "." + i);
            while (f.exists())
            {
                searchfiles.add(f);
                i ++;
                f = new File(s + "." + i);
            }
        }
        //File f = new File(commands.getOptionValue('i'));

        PrintWriter out;
        if (commands.hasOption('Z'))
        {
            out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('o'))))));
        }
        else
        {
            out = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('o')))), z)));
        }

        int maxDiff = Integer.parseInt(commands.getOptionValue('n',"0"));
        boolean just = commands.hasOption('j');

//        CountDataType dbt = new CountDataType();
        //ClosestInfoDataType<TreeCountMap<Integer>> cit = new ClosestInfoDataType<>(dbt);
//        DataPairDataType<KmerDiff, TreeCountMap<Integer>> innerPairDT = new DataPairDataType<>(new KmerDiffDataType(), new CountDataType("x","|"), "|");
//        SetDataType<DataPair<KmerDiff,TreeCountMap<Integer>>> setDT = new SetDataType<>(innerPairDT, " ");

        //int[] dataID = KmerFile.getDataID(f);

//        KmerFile.MetaData meta = KmerFile.getMetaData(f);
        KmerFile.MetaData meta = KmerFile.getMetaData(searchfiles.get(0));

        int minK = Integer.parseInt(commands.getOptionValue('k',Integer.toString(meta.minLength)));
        int maxK = Integer.parseInt(commands.getOptionValue('K',Integer.toString(meta.maxLength)));

        SetDataType<ReadPos> readDT = new SetDataType<>(new ReadPosDataType());
        //if (dataID == 1026) // If reads file against db
        if (Arrays.equals(meta.dataID, readDT.getID()))
        {

//            DataPairDataType<Set<ReadPos>, ClosestInfoCollector<TreeCountMap<Integer>>> rest = new DataPairDataType<>(rt,cit);
//            DataPairDataType<Set<ReadPos>, Set<DataPair<KmerDiff,TreeCountMap<Integer>>>> rest = new DataPairDataType<>(rt,setDT, "\t");
            //KmerWithDataDatatType<DataPair<Set<ReadPos>, ClosestInfoCollector<TreeCountMap<Integer>>>> kwdt = new KmerWithDataDatatType<>(rest);
//            KmerWithDataDataType<DataPair<Set<ReadPos>, Set<DataPair<KmerDiff,TreeCountMap<Integer>>>>> kwdt = new KmerWithDataDataType<>(rest, "\t");

            List<KmerFile<Set<ReadPos>>> sfiles = new ArrayList<>();
            for (File sf: searchfiles)
            {
                sfiles.add(new KmerFile<>(sf, readDT));
            }

            //doMatching(new KmerFile<>(f, readDT), db, out, ResultsDataType.getReadReferenceInstance(),
            doMatching(sfiles, db, out, ResultsDataType.getReadReferenceInstance(),
                    maxDiff, just, minK, maxK);
        }

        CountDataType referenceDT = new CountDataType();
        if (Arrays.equals(meta.dataID, referenceDT.getID()))
        {
            List<KmerFile<TreeCountMap<Integer>>> sfiles = new ArrayList<>();
            for (File sf: searchfiles)
            {
                sfiles.add(new KmerFile<>(sf, referenceDT));
            }

//            doMatching(new KmerFile<>(f, referenceDT), db, out, ResultsDataType.getReferenceReferenceInstance(),
            doMatching(sfiles, db, out, ResultsDataType.getReferenceReferenceInstance(),
                    maxDiff, just, minK, maxK);
        }

        out.close();

        System.out.println(sdf.format(new Date()));
    }

    private static <S,M> void doMatching(List<KmerFile<S>> searchFiles, DB<M> db, PrintWriter out,
                                         //KmerWithDataDatatType<DataPair<S, ClosestInfoCollector<M>>> kwdt,
                                         //KmerWithDataDataType<DataPair<S, Set<DataPair<KmerDiff,M>>>> kwdt,
                                         ResultsDataType<S,M> kwdt,
                                         int maxDiff, boolean just, int minK, int maxK) throws InconsistentDataException
    {
        //KmerStream<S> searchStream = searchFile.allKmers();
//        KmerStream<S> searchStream = searchFile.allRestrictedKmers(minK,maxK);
        KmerStream<S> searchStream = KmerStream.concatenateStreams(searchFiles.stream().map(sf -> sf.allRestrictedKmers(minK, maxK)));

        KmerStream<DataPair<S, Set<DataPair<KmerDiff,M>>>> resultStream =
                db.getNearestKmers(searchStream, maxDiff, just).filter(kwd -> !kwd.getData().getB().isEmpty());
        resultStream.forEach(kwd -> out.println(kwdt.toString(kwd)));
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
