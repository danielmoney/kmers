package Utils;

import CountMaps.TreeCountMap;
import DataTypes.*;
import Exceptions.UnknownTaxaException;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Kmers.KmerWithDataDataType;
import Reads.ReadPos;
import Reads.ReadPosDataType;
import Taxonomy.Tree;
import Taxonomy.Taxa;
import Zip.ZipHeader;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public class LCA
{
    public static void main(String[] args) throws IOException, ParseException
    {
        /*
        -i  Input file
        -t  Taxonomy
        -o  Output file
         */
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("i").required().hasArg().desc("Input file").build());
        options.addOption(Option.builder("x").required().hasArg().desc("Taxonomy file").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);

        ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> idt = ResultsDataType.getReadReferenceInstance();
        ResultsFile<Set<ReadPos>, TreeCountMap<Integer>> in = new ResultsFile<>(new File(commands.getOptionValue('i')), idt);

        Tree t = Tree.getInstanceFromFile(new File(commands.getOptionValue('x')));

        PrintWriter out = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                new FileOutputStream(new File(commands.getOptionValue('o')))), 5)));
        KmerWithDataDataType<DataPair<Set<ReadPos>, Map<Integer,Integer>>> odt = new KmerWithDataDataType<>(
                new DataPairDataType<>(new SetDataType<>(new ReadPosDataType(),"|"), new MapDataType<>(new IntDataType(), new IntDataType(), ":", "|"),"\t"));

        in.stream().map(kwd -> lca(kwd,t)).forEach(kwd -> out.println(odt.toString(kwd)));

        in.close();
        out.close();

        System.out.println(sdf.format(new Date()));
    }


    public static <S> KmerWithData<DataPair<S, Map<Integer,Integer>>> lca(
            KmerWithData<DataPair<S,Set<DataPair<KmerDiff,TreeCountMap<Integer>>>>> input,
            Tree t)
    {
        Map<Integer, List<Taxa>> distTaxa = new TreeMap<>();
        for (DataPair<KmerDiff,TreeCountMap<Integer>> dp: input.getData().getB())
        {
            int d = dp.getA().dist();
            if (!distTaxa.containsKey(d))
            {
                distTaxa.put(d, new LinkedList<>());
            }
            for (Integer i: dp.getB().keySet())
            {

                try
                {
                    distTaxa.get(d).add(t.getNode(i));
                }
                catch (UnknownTaxaException e)
                {
                    return null;
                }
            }
        }

        Taxa lastLCA = null;

        Map<Integer,Integer> ret = new TreeMap<>();
        for (Map.Entry<Integer, List<Taxa>> e: distTaxa.entrySet())
        {
            if (lastLCA != null)
            {
                e.getValue().add(lastLCA);
            }
            Taxa lca = t.getLCA(e.getValue());
            ret.put(e.getKey(), lca.getID());
            lastLCA = lca;
        }
        return new KmerWithData<>(input.getKmer(),new DataPair<>(input.getData().getA(),ret));
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}
