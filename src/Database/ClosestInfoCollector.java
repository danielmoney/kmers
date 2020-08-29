package Database;

import DataTypes.DataPair;
import Kmers.Kmer;
import Kmers.KmerDiff;
import Kmers.KmerWithData;

import java.util.*;

public class ClosestInfoCollector<M>
{
    public ClosestInfoCollector()
    {
        matchedKmers = new TreeMap<>();
        mindist = 255;
    }

    public ClosestInfoCollector(KmerWithData<M> kwd)
    {
        matchedKmers = new TreeMap<>();
        matchedKmers.put(kwd.getKmer(), new CI<>(kwd,(byte) 0));
        mindist = 0;
    }

    public void add(KmerWithData<M> kwd, byte dist)
    {
        add(new CI<>(kwd,dist));
    }

    public void add(CI<M> ci)
    {
        // Need to do rc check here I think!!!
        Kmer k = ci.kwd.getKmer();
        Kmer rc = k.getRC();

        if (matchedKmers.containsKey(rc))
        {
            CI<M> rcci = matchedKmers.get(rc);
            if (ci.getDist() < rcci.getDist())
            {
                matchedKmers.remove(rc);
                matchedKmers.put(k,ci);
            }
        }
        else
        {
            matchedKmers.put(k, ci);
        }
        mindist = Math.min(ci.dist, mindist);
    }

    public void addAll(ClosestInfoCollector<M> oci)
    {
         for (CI<M> ci : oci.getMatchedKmers())
        {
            add(ci);
        }
        mindist = Math.min(mindist,oci.getMinDist());
    }

    // NOT SURE WHETHER THIS SHOULD EXIST OR WETHER WE SHOULD JUST RETURN CIs
    public Collection<CI<M>> getMatchedKmers()
    {
        return matchedKmers.values();
    }

    public int getMinDist()
    {
        return mindist;
    }

    public boolean hasMatches()
    {
        return !matchedKmers.isEmpty();
    }

    public String toString()
    {
        return mindist + "\t" + matchedKmers.toString();
    }

    public Set<DataPair<KmerDiff,M>> getResult(Kmer search)
    {
        Set<DataPair<KmerDiff,M>> result = new TreeSet<>(new KmerDiffComparaotr());

        for (Map.Entry<Kmer,CI<M>> e: matchedKmers.entrySet())
        {
            result.add(new DataPair<>(new KmerDiff(search,e.getKey()), e.getValue().kwd.getData()));
        }

        return result;
    }

    private Map<Kmer,CI<M>> matchedKmers;
    private int mindist;

    public static class CI<D>
    {
        public CI(KmerWithData<D> kwd, byte dist)
        {
            this.kwd = kwd;
            this.dist = dist;
        }

        public KmerWithData<D> getKWD()
        {
            return kwd;
        }

        public byte getDist()
        {
            return dist;
        }

        private KmerWithData<D> kwd;
        private byte dist;
    }

    public static class KmerDiffComparaotr implements Comparator<DataPair<KmerDiff,?>>
    {
        public int compare(DataPair<KmerDiff,?> p1, DataPair<KmerDiff,?> p2)
        {
            List<KmerDiff.Diff> kd1 = p1.getA().getDiffs();
            List<KmerDiff.Diff> kd2 = p2.getA().getDiffs();

            int c = Integer.compare(kd1.size(),kd2.size());
            if (c != 0)
            {
                return c;
            }

            for (int i = 0; i < kd1.size(); i++)
            {
                c = Byte.compare(kd1.get(i).getPosition(), kd2.get(i).getPosition());
                if (c != 0)
                {
                    return c;
                }
                c = Byte.compare(kd1.get(i).getBase().pos(), kd2.get(i).getBase().pos());
                if (c != 0)
                {
                    return c;
                }
            }
            return 0;
        }
    }
}
