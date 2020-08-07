package Database;

import Kmers.KmerWithData;

import java.util.Map;

public class ClosestInfo<S,M>
{
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

    public void merge(ClosestInfo oci)
    {
        //Should probably throw an error if the search kmer is not the same
        //Should probably throw error if dists are not the same...
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
