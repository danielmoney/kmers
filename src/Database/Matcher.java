package Database;

import CountMaps.TreeCountMap;
import Counts.CountDataType;
import DataTypes.DataCollector;
import KmerFiles.KmerFile;
import Reads.ReadPos;
import Reads.ReadPosSetDataType;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
            dbfiles.add(new KmerFile<>(new File(a.get(i)), new CountDataType()));
        }
        DB<TreeCountMap<Integer>> db = new DB<>(dbfiles);

        File f = new File(a.get(1));
        int dataID = KmerFile.getDataID(f);
        if (dataID == 1026) // If reads file against db
        {
            KmerFile<Set<ReadPos>> matchFile = new KmerFile<>(f,
                    new ReadPosSetDataType());
        }

        // Output file - maybe pos 1?
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
