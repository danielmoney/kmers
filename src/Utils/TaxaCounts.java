package Utils;

import CountMaps.TreeCountMap;
import CountMaps.HashCountMap;
import Counts.CountDataType;
import Database.DB;
import Exceptions.InconsistentDataException;
import Exceptions.UnknownTaxaException;
import KmerFiles.KmerFile;
import Taxonomy.Taxa;
import Taxonomy.Tree;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TaxaCounts
{
    public static void main(String[] args) throws IOException, InconsistentDataException, ExecutionException, InterruptedException
    {
        List<KmerFile<TreeCountMap<Integer>>> dbfiles = new LinkedList<>();
        dbfiles.add(new KmerFile<>(new File("norway_cr.db.gz"), new CountDataType()));
        DB<TreeCountMap<Integer>> db = new DB<>(dbfiles);

        ExecutorService ex = Executors.newFixedThreadPool(4);
        int maxKey = db.getMaxKey();

        maxKey = 4;

        if (false)
        {
            Tree t = Tree.getInstanceFromFile(new File("../taxonomy/taxonomy.dat"));
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
                System.out.print(i + "\t" + totals.match.getCount(i) + "\t" + totals.unique.getCount(i));
                if (t != null)
                {
                    System.out.print("\t" + totals.lca.getCount(i) + "\t" + totals.child.getCount(i));
                }
                System.out.println();
            }
        }
        else
        {
            List<KmerPattern> patterns = new LinkedList<>();
            patterns.add(new KmerPattern("81729 137457 + #Test1"));
            patterns.add(new KmerPattern("81729 !137457 + #Test2"));
            patterns.add(new KmerPattern("!81729 137457 + #Test3"));
            patterns.add(new KmerPattern("81729 + #Test4"));
            patterns.add(new KmerPattern("137457 + #Test5"));
            patterns.add(new KmerPattern("81729 #Test6"));
            patterns.add(new KmerPattern("137457 #Test7"));

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
                System.out.println(p.name + "\t" + totals.counts.getCount(p));
            }
        }

        ex.shutdown();
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
            lca = new TreeCountMap<>();
            child = new TreeCountMap<>();
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
}