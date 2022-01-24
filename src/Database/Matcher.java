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
import Streams.StreamUtils;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
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

        options.addOption(Option.builder("L").hasArg().desc("Limit keys").build());

        OptionGroup zipoptions = new OptionGroup();
        zipoptions.addOption(Option.builder("z").hasArg().desc("Zip compression level").build());
        zipoptions.addOption(Option.builder("Z").hasArg().desc("Unzipped output").build());
        options.addOptionGroup(zipoptions);

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //This is a bit hacky but is better than nothing
        CommandLine commands = null;
        try
        {
            commands = parser.parse(options, args);
        }
        catch (MissingOptionException | MissingArgumentException | UnrecognizedOptionException ex)
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -cp Kmers.jar Kmers.Matcher -i INPUT -d DATABASE -o OUTPUT <options>", options);
            System.exit(1);
        }


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
                boolean found = false;
                int i = 1;
                f = new File(s + "." + i);
                while (f.exists())
                {
                    dbfiles.add(new KmerFile<>(f, new CountDataType()));
                    i ++;
                    f = new File(s + "." + i);
                    found = true;
                }
                if (!found)
                {
                    throw new FileNotFoundException(s);
                }
            }
        }
        DB<TreeCountMap<Integer>> db = new DB<>(dbfiles);

        if (commands.hasOption("t"))
        {
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

        KmerFile.MetaData meta = KmerFile.getMetaData(searchfiles.get(0));

        int minK = Integer.parseInt(commands.getOptionValue('k',Integer.toString(meta.minLength)));
        int maxK = Integer.parseInt(commands.getOptionValue('K',Integer.toString(meta.maxLength)));

        SetDataType<ReadPos> readDT = new SetDataType<>(new ReadPosDataType());
        if (Arrays.equals(meta.dataID, readDT.getID()))
        {
            List<KmerFile<Set<ReadPos>>> sfiles = new ArrayList<>();
            for (File sf: searchfiles)
            {
                sfiles.add(new KmerFile<>(sf, readDT));
            }

            if (commands.hasOption('L'))
            {
                String[] parts = commands.getOptionValue('L').split("-");
                int start = Integer.parseInt(parts[0]);
                int end;
                if (parts.length == 2)
                {
                    end = Integer.parseInt(parts[1]) + 1;
                }
                else
                {
                    end = start + 1;
                }
                doMatching(sfiles, db, out, ResultsDataType.getReadReferenceInstance(),
                        maxDiff, just, minK, maxK, start, end);
            }
            else
            {
                doMatching(sfiles, db, out, ResultsDataType.getReadReferenceInstance(),
                        maxDiff, just, minK, maxK);
            }
        }

        CountDataType referenceDT = new CountDataType();
        if (Arrays.equals(meta.dataID, referenceDT.getID()))
        {
            List<KmerFile<TreeCountMap<Integer>>> sfiles = new ArrayList<>();
            for (File sf: searchfiles)
            {
                sfiles.add(new KmerFile<>(sf, referenceDT));
            }

            if (commands.hasOption('L'))
            {
                String[] parts = commands.getOptionValue('L').split("-");
                int start = Integer.parseInt(parts[0]);
                int end;
                if (parts.length == 2)
                {
                    end = Integer.parseInt(parts[1]) + 1;
                }
                else
                {
                    end = start + 1;
                }
                doMatching(sfiles, db, out, ResultsDataType.getReferenceReferenceInstance(),
                        maxDiff, just, minK, maxK, start, end);
            }

            doMatching(sfiles, db, out, ResultsDataType.getReferenceReferenceInstance(),
                    maxDiff, just, minK, maxK);
        }

        out.close();

        System.out.println(sdf.format(new Date()));
    }

    private static <S,M> void doMatching(List<KmerFile<S>> searchFiles, DB<M> db, PrintWriter out,
                                         ResultsDataType<S,M> kwdt,
                                         int maxDiff, boolean just, int minK, int maxK,
                                         int startKey, int endKey) throws InconsistentDataException
    {
        //KmerStream<S> searchStream = searchFiles.get(0).allKmers();
        KmerStream<S> searchStream = new KmerStream<>(
            StreamUtils.mergeSortedStreams(searchFiles.stream().map(f -> f.restrictedKmers(startKey, endKey, minK, maxK).stream()).collect(Collectors.toList()),
                    (kwd1,kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())),
            minK,
            maxK,
            true);

        KmerStream<DataPair<S, Set<DataPair<KmerDiff,M>>>> resultStream =
                db.getNearestKmers(searchStream, maxDiff, just).filter(kwd -> !kwd.getData().getB().isEmpty());
        resultStream.forEach(kwd -> out.println(kwdt.toString(kwd)));
    }

    private static <S,M> void doMatching(List<KmerFile<S>> searchFiles, DB<M> db, PrintWriter out,
                                         ResultsDataType<S,M> kwdt,
                                         int maxDiff, boolean just, int minK, int maxK) throws InconsistentDataException
    {
        //KmerStream<S> searchStream = KmerStream.concatenateStreams(searchFiles.stream().map(sf -> sf.allRestrictedKmers(minK, maxK)));
        KmerStream<S> searchStream = new KmerStream<>(
                StreamUtils.mergeSortedStreams(searchFiles.stream().map(f -> f.allRestrictedKmers(minK, maxK).stream()).collect(Collectors.toList()),
                        (kwd1,kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer())),
                minK,
                maxK,
                true);

        KmerStream<DataPair<S, Set<DataPair<KmerDiff,M>>>> resultStream =
                db.getNearestKmers(searchStream, maxDiff, just).filter(kwd -> !kwd.getData().getB().isEmpty());
        resultStream.forEach(kwd -> out.println(kwdt.toString(kwd)));
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
