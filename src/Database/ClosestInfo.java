package Database;

import Kmers.KmerWithData;

import java.util.Map;
import java.util.TreeMap;

public class ClosestInfo<S,M>
{
    public ClosestInfo(KmerWithData<S> searchKmerm, KmerWithData<M> matchedKmer)
    {
        this.searchKmer = searchKmer;
        matchedKmers = new TreeMap<>();
        matchedKmers.put(matchedKmer,0);
        mindist = 0;
    }

    public ClosestInfo(KmerWithData<S> searchKmer, Map<KmerWithData<M>,Integer> matchedKmers, int mindist)
    {
        this.searchKmer = searchKmer;
        this.matchedKmers = matchedKmers;
        this.mindist = mindist;
    }

    public KmerWithData<S> getSearchKmer()
    {
        return searchKmer;
    }

    public Map<KmerWithData<M>,Integer> getMatchedKmers()
    {
        return matchedKmers;
    }

    public int getMinDist()
    {
        return mindist;
    }

    public void merge(ClosestInfo<S,M> oci)
    {
        //Should probably throw an error if the search kmer is not the same
        /**********
         * Need to check we don't merge in a rc of something already there
         */
        matchedKmers.putAll(oci.getMatchedKmers());
        mindist = Math.min(mindist,oci.getMinDist());
    }

    public String toString()
    {
        return searchKmer.toString() + "\t" + mindist + "\t" + matchedKmers.toString();
    }

    private KmerWithData<S> searchKmer;
    private Map<KmerWithData<M>,Integer> matchedKmers;
    private int mindist;
}
