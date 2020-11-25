package Utils;

import CountMaps.TreeCountMap;
import CountMaps.HashCountMap;
import Counts.CountDataType;
import Database.DB;
import Exceptions.InconsistentDataException;
import Exceptions.UnknownTaxaException;
import KmerFiles.KmerFile;
import SumMaps.TreeSumMap;
import Taxonomy.Taxa;
import Taxonomy.Tree;
import Zip.ZipOrNot;
import org.apache.commons.cli.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TaxaCounts
{
    public static void main(String[] args) throws IOException, InconsistentDataException, ExecutionException, InterruptedException, ParseException
    {
        /*
        -d  Database file
        -t  Taxonomy file
        -s  Patterns
        -o  Output file - Currently prints to screen
         */
        System.out.println(sdf.format(new Date()));

        Options options = new Options();

        options.addOption(Option.builder("d").required().hasArg().desc("Database file").build());
        options.addOption(Option.builder("x").hasArg().desc("Taxonomy file (optional)").build());
        options.addOption(Option.builder("s").hasArg().desc("Search taxa file (optional)").build());
        options.addOption(Option.builder("P").hasArg().desc("Prune reference file (optional)").build());
        options.addOption(Option.builder("o").required().hasArg().desc("Output file").build());

        options.addOption(Option.builder("N").hasArg().desc("Number of samples").build());

        options.addOption(Option.builder("t").hasArg().desc("Number of threads to use").build());

        CommandLineParser parser = new DefaultParser();

        //Obviously neeed to do something better here than just throw the ParseException!
        CommandLine commands = parser.parse(options, args);


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
            db.setThreads(Integer.parseInt(commands.getOptionValue('t')));
        }

        ExecutorService ex = Executors.newFixedThreadPool(4);
        int maxKey = db.getMaxKey();

        maxKey = 4;

        PrintWriter out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(
                    new FileOutputStream(new File(commands.getOptionValue('o'))))));

        if (!commands.hasOption('s'))
        {
            Tree t;
            if (commands.hasOption('x'))
            {
                t = Tree.getInstanceFromFile(new File(commands.getOptionValue('x')));
            }
            else
            {
                t = null;
            }

            if (!commands.hasOption('P'))
            {
                ArrayList<Future<CountDataAll>> futures = new ArrayList<>(maxKey);

                for (int i = 0; i < maxKey; i++)
                {
                    futures.add(ex.submit(new SingleIndexAll(db, i, t)));
                }

                CountDataAll totals = new CountDataAll(t);
                for (Future<CountDataAll> cd : futures)
                {
                    totals.mergeIn(cd.get());
                }

                Set<Integer> keySet;
                if (t == null)
                {
                    keySet = totals.match.keySet();
                }
                else
                {
                    keySet = totals.child.keySet();
                }

                for (int i : keySet)
                {
                    out.print(i + "\t" + totals.match.getCount(i) + "\t" + totals.unique.getCount(i));
                    if (t != null)
                    {
                        out.print("\t" + totals.lca.getCount(i) + "\t" + totals.child.getCount(i));
                    }
                    out.println();
                }
            }
            else
            {
                int numSamples = Integer.parseInt(commands.getOptionValue('N', "1000"));
                ArrayList<Future<CountDataSampled>> futures = new ArrayList<>(maxKey);

                Map<Integer,Double> sampleProb = new TreeMap<>();
                BufferedReader in = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('P')));
                String line = in.readLine();
                while (line != null)
                {
                    String[] parts = line.split("\t");
                    sampleProb.put(Integer.valueOf(parts[0]), Double.valueOf(parts[1]));
                    line = in.readLine();
                }


                for (int i = 0; i < maxKey; i++)
                {
                    futures.add(ex.submit(new SingleIndexSampled(db, i, t, sampleProb, numSamples)));
                }

                CountDataSampled totals = new CountDataSampled(sampleProb, t, numSamples);
                for (Future<CountDataSampled> cd : futures)
                {
                    totals.mergeIn(cd.get());
                }

                Set<Integer> keySet;
                keySet = new TreeSet<>(totals.match.keySet());
                if (t != null)
                {
                    // Hmmmm, not sure child is right for this!!
                    keySet.addAll(totals.child.keySet());
                }

                for (int i : keySet)
                {
                    out.print(i + "\t" + totals.match.getValue(i) + "\t" + totals.unique.getValue(i));
                    if (t != null)
                    {
                        out.print("\t" + totals.lca.getValue(i) + "\t" + totals.child.getValue(i));
                    }
                    out.println();
                }
            }
        }
        else
        {
            List<KmerPattern> patterns = new LinkedList<>();
            BufferedReader pfile = ZipOrNot.getBufferedReader(new File(commands.getOptionValue('s')));
            pfile.lines().forEach(l -> patterns.add(new KmerPattern(l)));
            pfile.close();

            ArrayList<Future<CountDataSpecific>> futures = new ArrayList<>(maxKey);

            for (int i = 0; i < maxKey; i++)
            {
                futures.add(ex.submit(new SingleIndexSpecific(db, i, patterns)));
            }

            CountDataSpecific totals = new CountDataSpecific(patterns);
            for (Future<CountDataSpecific> cd : futures)
            {
                totals.mergeIn(cd.get());
            }

            for (KmerPattern p: patterns)
            {
                out.println(p.name + "\t" + totals.counts.getCount(p));
            }
        }

        ex.shutdown();
        out.close();

        System.out.println(sdf.format(new Date()));
    }

    private static class KmerPattern
    {
        private KmerPattern(String s)
        {
            present = new LinkedList<>();
            absent = new LinkedList<>();
            allowOthers = false;

            String[] parts = s.split("\\s");
            for (String p: parts)
            {
                switch (p.charAt(0))
                {
                    case '!':
                        absent.add(Integer.parseInt(p.substring(1)));
                        break;
                    case '+':
                        allowOthers = true;
                        break;
                    case '#':
                        name = p.substring(1);
                        break;
                    default:
                        present.add(Integer.parseInt(p));
                        break;
                }
            }
        }

        private boolean test(Set<Integer> taxaSet)
        {
            for (Integer p: present)
            {
                if (!taxaSet.contains(p))
                {
                    return false;
                }
            }

            for (Integer a: absent)
            {
                if (taxaSet.contains(a))
                {
                    return false;
                }
            }

            if (!allowOthers)
            {
                for (Integer t: taxaSet)
                {
                    if (!present.contains(t))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        String name;
        List<Integer> present;
        List<Integer> absent;
        boolean allowOthers;
    }

    private static class SingleIndexSpecific implements Callable<CountDataSpecific>
    {
        private SingleIndexSpecific(DB<TreeCountMap<Integer>> db, int index, List<KmerPattern> patterns)
        {
            this.db = db;
            this.index = index;
            this.patterns = patterns;
        }

        public CountDataSpecific call()
        {
            CountDataSpecific cd = new CountDataSpecific(patterns);
            db.kmers(index).forEach(kwd -> cd.add(kwd.getData().keySet()));
            return cd;
        }

        List<KmerPattern> patterns;
        DB<TreeCountMap<Integer>> db;
        int index;
    }

    private static class CountDataSpecific
    {
        private CountDataSpecific(List<KmerPattern> patterns)
        {
            this.patterns = patterns;
            counts = new HashCountMap<>();
        }

        private void add(Set<Integer> taxa)
        {
            for (KmerPattern kp: patterns)
            {
                if (kp.test(taxa))
                {
                    counts.add(kp);
                }
            }
        }

        private void mergeIn(CountDataSpecific other)
        {
            counts.addAll(other.counts);
        }

        private List<KmerPattern> patterns;
        private HashCountMap<KmerPattern> counts;
    }

    private static class SingleIndexAll implements Callable<CountDataAll>
    {
        private SingleIndexAll(DB<TreeCountMap<Integer>> db, int index, Tree t)
        {
            this.db = db;
            this.index = index;
            this.tree = t;
        }

        public CountDataAll call()
        {
            CountDataAll cd = new CountDataAll(tree);
            db.kmers(index).forEach(kwd -> cd.add(kwd.getData().keySet()));
            return cd;
        }

        private DB<TreeCountMap<Integer>> db;
        private int index;
        private Tree tree;
    }

    private static class CountDataAll
    {
        private CountDataAll()
        {
            match = new TreeCountMap<>();
            unique = new TreeCountMap<>();
        }

        private CountDataAll(Tree t)
        {
            this();
            this.t = t;
            if (t != null)
            {
                lca = new TreeCountMap<>();
                child = new TreeCountMap<>();
            }
        }

        private void add(Set<Integer> taxa)
        {
            for (Integer i: taxa)
            {
                match.add(i);
            }
            if (taxa.size() == 1)
            {
                unique.add(taxa.iterator().next());
            }
            if (t != null)
            {
                Set<Taxa> tt = taxa.stream().map(i -> {
                    try
                    {
                        return t.getNode(i);
                    }
                    catch (UnknownTaxaException e)
                    {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toSet());
                Taxa lastCommon = t.getLCA(tt);
                lca.add(lastCommon.getID());

                int curID = lastCommon.getID();
                while (curID != -1)
                {
                    child.add(curID);
                    try
                    {
                        curID = t.getNode(curID).getParentID();
                    }
                    catch (UnknownTaxaException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }

        private void mergeIn(CountDataAll other)
        {
            match.addAll(other.match);
            unique.addAll(other.unique);
            if (t!= null)
            {
                lca.addAll(other.lca);
                child.addAll(other.child);
            }
        }

        private TreeCountMap<Integer> lca;
        private TreeCountMap<Integer> match;
        private TreeCountMap<Integer> unique;
        private TreeCountMap<Integer> child;
        private Tree t = null;
    }

    private static class SingleIndexSampled implements Callable<CountDataSampled>
    {
        private SingleIndexSampled(DB<TreeCountMap<Integer>> db, int index, Tree t, Map<Integer,Double> sampleProb, int numSamples)
        {
            this.db = db;
            this.index = index;
            this.tree = t;
            this.sampleProb = sampleProb;
            this.numSamples = numSamples;
        }

        public CountDataSampled call()
        {
            CountDataSampled cd = new CountDataSampled(sampleProb, tree, numSamples);
            db.kmers(index).forEach(kwd -> cd.add(kwd.getData().keySet()));
            return cd;
        }

        private DB<TreeCountMap<Integer>> db;
        private int index;
        private Tree tree;
        private Map<Integer,Double> sampleProb;
        private int numSamples;
    }

    private static class CountDataSampled
    {
        private CountDataSampled(Map<Integer,Double> sampleProb)
        {
            this.sampleProb = sampleProb;

            match = new TreeSumMap<>();
            unique = new TreeSumMap<>();

            matchVar = new TreeSumMap<>();
            uniqueVar = new TreeSumMap<>();
        }

        private CountDataSampled(Map<Integer,Double> sampleProb, Tree t, int numSamples)
        {
            this(sampleProb);

            this.t = t;

            if (t != null)
            {
                lca = new TreeSumMap<>();
                child = new TreeSumMap<>();

                lcaVar = new TreeSumMap<>();
                childVar = new TreeSumMap<>();
            }

            r = new Random();
            samples = numSamples;
        }

        private void add(Set<Integer> taxa)
        {
            int always = 0;
            for (Integer i: taxa)
            {
                double s = taxprob(i);
                match.add(i, s);
                matchVar.add(i, s * (1-s));
                if (s == 1.0)
                {
                    always++;
                }
            }
            if (always == 0)
            {
                // Any of the taxa could be the present one
                double noneP = 1.0;
                for (Integer i: taxa)
                {
                    noneP *= (1.0 - taxprob(i));
                }
                for (Integer i: taxa)
                {
                    double s = taxprob(i);
                    double p = noneP * (s / (1-s));
                    unique.add(i, p);
                    uniqueVar.add(i, p * (1-p));
                }
            }
            if (always == 1)
            {
                // Only the always present one can be the single taxa
                double p = 1.0;
                int a = -1;
                for (Integer i: taxa)
                {
                    double s = taxprob(i);
                    p *= s;
                    if (s == 1.0)
                    {
                        a = i;
                    }
                }

                unique.add(a, p);
                uniqueVar.add(a, p * (1-p));
            }
            if (t != null)
            {
                for (int i = 0; i < samples; i++)
                {
                    Set<Integer> sampled = sampletaxa(taxa);
                    if (!sampled.isEmpty())
                    {
                        Set<Taxa> tt = sampled.stream().map(j -> {
                            try
                            {
                                return t.getNode(j);
                            }
                            catch (UnknownTaxaException e)
                            {
                                return null;
                            }
                        }).filter(Objects::nonNull).collect(Collectors.toSet());
                        Taxa lastCommon = t.getLCA(tt);
                        lca.add(lastCommon.getID(), 1.0 / samples);

                        int curID = lastCommon.getID();
                        while (curID != -1)
                        {
                            child.add(curID, 1.0 / samples);
                            try
                            {
                                curID = t.getNode(curID).getParentID();
                            }
                            catch (UnknownTaxaException e)
                            {
                                e.printStackTrace();
                            }
                        }
                    }
                }

//                Set<Taxa> tt = taxa.stream().map(i -> {
//                    try
//                    {
//                        return t.getNode(i);
//                    }
//                    catch (UnknownTaxaException e)
//                    {
//                        return null;
//                    }
//                }).filter(Objects::nonNull).collect(Collectors.toSet());
//                Taxa lastCommon = t.getLCA(tt);
//                lca.add(lastCommon.getID());
//
//                int curID = lastCommon.getID();
//                while (curID != -1)
//                {
//                    child.add(curID);
//                    try
//                    {
//                        curID = t.getNode(curID).getParentID();
//                    }
//                    catch (UnknownTaxaException e)
//                    {
//                        e.printStackTrace();
//                    }
//                }
            }
        }

        private Set<Integer> sampletaxa(Set<Integer> taxa)
        {
            TreeSet<Integer> sample = new TreeSet<>();
            for (Integer t: taxa)
            {
                if (taxprob(t) < r.nextDouble())
                {
                    sample.add(t);
                }
            }
            return sample;
        }

        private double taxprob(int id)
        {
            if (sampleProb.containsKey(id))
            {
                return sampleProb.get(id);
            }
            else
            {
                return 1.0;
            }
        }

        private void mergeIn(CountDataSampled other)
        {
            match.addAll(other.match);
            unique.addAll(other.unique);
            if (t!= null)
            {
                lca.addAll(other.lca);
                child.addAll(other.child);
            }
        }

        private TreeSumMap<Integer> lca;
        private TreeSumMap<Integer> match;
        private TreeSumMap<Integer> unique;
        private TreeSumMap<Integer> child;

        private TreeSumMap<Integer> lcaVar;
        private TreeSumMap<Integer> matchVar;
        private TreeSumMap<Integer> uniqueVar;
        private TreeSumMap<Integer> childVar;

        private Tree t = null;
        private Random r = null;
        private int samples;

        private Map<Integer,Double> sampleProb;
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}