package Database;

import Compression.IntCompressor;
import CountMaps.TreeCountMap;
import DataTypes.DataType;
import IndexedFiles.IndexedInputFile;
import IndexedFiles.StandardIndexedInputFile;
import IndexedFiles.ZippedIndexedInputFile;
import KmerFiles.CountCompressor;
import KmerFiles.KmerFile;
import Kmers.KmerWithData;
import Kmers.KmerWithDataStreamWrapper;
import Reads.ReadPos;
import Reads.ReadPosSetCompressor;
import Streams.StreamUtils;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class Matcher
{
    public void main(String[] args) throws ParseException, IOException
    {
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("z").hasArg().desc("Zip compression level").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        List<String> a = commands.getArgList();
        int z = Integer.parseInt(commands.getOptionValue('z', "5"));

        List<KmerFile<TreeCountMap<Integer>>> dbfiles = new LinkedList<>();
        for (int i = 1; i < a.size(); i ++)
        {
            dbfiles.add(new KmerFile<>(getIndexedInputFile(new File(a.get(i))), DataType.getCountInstance()));
        }
        DB<TreeCountMap<Integer>> db = new DB<>(dbfiles);//, (cm1, cm2) -> {cm1.addAll(cm2); return cm1;});

        IndexedInputFile<Integer> f = getIndexedInputFile(new File(a.get(1)));
        int dataID = KmerFile.getDataID(f);
        if (dataID == 1026) // If reads file against db
        {
            KmerFile<Set<ReadPos>> matchFile = new KmerFile<>(f,
                    //new ReadPosSetCompressor());
                    DataType.getReadPosInstance());
        }

        // Output file - maybe pos 1?
    }

    private static <S,M> Stream<ClosestInfo<S,M>> match(KmerWithDataStreamWrapper<S> searchStream, DB<M> db, int maxDiff, boolean just)
    {
        if (false) // If we can do fast matching
        {
            return StreamUtils.matchTwoStreams(searchStream.stream(),db.allKmers().stream(),
                        //(kwd1, kwd2) -> kwd1.toString() + "\t" + kwd2.toString(),
                        (kwd1, kwd2) -> new ClosestInfo<>(kwd1,kwd2),
                        (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()));
        }
        else
        {
            return db.getNearestKmers(searchStream, maxDiff, just);
        }
    }

    private static IndexedInputFile<Integer> getIndexedInputFile(File f) throws IOException
    {
        if (ZipOrNot.isGZipped(f))
        {
            return new ZippedIndexedInputFile<>(f, new IntCompressor());
        }
        else
        {
            return new StandardIndexedInputFile<>(f, new IntCompressor());
        }
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
