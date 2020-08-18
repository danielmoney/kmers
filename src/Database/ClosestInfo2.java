package Database;

import Kmers.Kmer;
import Kmers.KmerWithData;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class ClosestInfo2<M>
{
    public ClosestInfo2()
    {
        matchedKmers = new TreeMap<>();
        mindist = 255;
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

    public void addAll(ClosestInfo2<M> oci)
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

    public String toString()
    {
        return mindist + "\t" + matchedKmers.toString();
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
}
