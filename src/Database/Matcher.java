package Database;

import Concurrent.LimitedQueueExecutor;
import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.DataPair;
import DataTypes.DataPairDataType;
import Exceptions.InconsistentDataException;
import KmerFiles.KmerFile;
import Kmers.KmerStream;
import Kmers.KmerWithDataDatatType;
import Reads.ReadPos;
import Reads.ReadPosSetDataType;
import Zip.ZipOrNot;
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

        if (commands.hasOption("t"))
        {
            LimitedQueueExecutor.setDefaultNumberThreads(Integer.parseInt(commands.getOptionValue('t')));
        }

        List<KmerFile<TreeCountMap<Integer>>> dbfiles = new LinkedList<>();
        for (String s: commands.getOptionValues('d'))
        {
            dbfiles.add(new KmerFile<>(new File(s), new CountDataType()));
        }
        DB<TreeCountMap<Integer>> db = new DB<>(dbfiles);

        File f = new File(commands.getOptionValue('i'));

        PrintWriter out;
        if (commands.hasOption('Z'))
        {
            out = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('o')))), z)));
        }
        else
        {
            out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('o'))))));
        }

        int maxDiff = Integer.parseInt(commands.getOptionValue('n',"0"));
        boolean just = commands.hasOption('j');

        CountDataType dbt = new CountDataType();
        ClosestInfoDataType<TreeCountMap<Integer>> cit = new ClosestInfoDataType<>(dbt);

        //int[] dataID = KmerFile.getDataID(f);

        KmerFile.MetaData meta = KmerFile.getMetaData(f);

        int minK = Integer.parseInt(commands.getOptionValue('l',Integer.toString(meta.minLength)));
        int maxK = Integer.parseInt(commands.getOptionValue('k',Integer.toString(meta.maxLength)));

        ReadPosSetDataType rt = new ReadPosSetDataType();
        //if (dataID == 1026) // If reads file against db
        if (Arrays.equals(meta.dataID, rt.getID()))
        {

            DataPairDataType<Set<ReadPos>, ClosestInfo<TreeCountMap<Integer>>> rest = new DataPairDataType<>(rt,cit);
            KmerWithDataDatatType<DataPair<Set<ReadPos>, ClosestInfo<TreeCountMap<Integer>>>> kwdt = new KmerWithDataDatatType<>(rest);

            doMatching(new KmerFile<>(f, new ReadPosSetDataType()), db, out, kwdt,
                    maxDiff, just, minK, maxK);
        }

        out.close();

        System.out.println(sdf.format(new Date()));
    }

    private static <S,M> void doMatching(KmerFile<S> searchFile, DB<M> db, PrintWriter out,
                                         KmerWithDataDatatType<DataPair<S,ClosestInfo<M>>> kwdt,
                                         int maxDiff, boolean just, int minK, int maxK) throws InconsistentDataException
    {
        //KmerStream<S> searchStream = searchFile.allKmers();
        KmerStream<S> searchStream = searchFile.allRestrictedKmers(minK,maxK);

        KmerStream<DataPair<S, ClosestInfo<M>>> resultStream =
                db.getNearestKmers(searchStream, maxDiff, just).filter(kwd -> kwd.getData().getB().hasMatches());
        resultStream.forEach(kwd -> out.println(kwdt.toString(kwd)));
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
