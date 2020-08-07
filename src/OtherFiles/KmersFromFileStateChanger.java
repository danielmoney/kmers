package OtherFiles;

import java.util.EnumMap;
import java.util.Map;
import java.util.TreeMap;

public class KmersFromFileStateChanger
{
    public KmersFromFileStateChanger(KmersFromFileState startState)
    {
        this.startState = startState;
        mapping = new EnumMap<>(KmersFromFileState.class);
        for (KmersFromFileState s: KmersFromFileState.values())
        {
            mapping.put(s,new TreeMap<>());
        }
    }

    public void add(KmersFromFileState startState, char character, KmersFromFileState endState)
    {
        mapping.get(startState).put((int) character, endState);
    }

    public KmersFromFileState get(KmersFromFileState startState, int character)
    {
        return mapping.get(startState).get(character);
    }

    public KmersFromFileState startState()
    {
        return startState;
    }

    private Map<KmersFromFileState, Map<Integer, KmersFromFileState>> mapping;
    private KmersFromFileState startState;

    public static KmersFromFileStateChanger getFAinstance()
    {
        KmersFromFileStateChanger stateChanger = new KmersFromFileStateChanger(KmersFromFileState.ID);
        stateChanger.add(KmersFromFileState.ID, '\t', KmersFromFileState.KMER);
        stateChanger.add(KmersFromFileState.ID, ' ', KmersFromFileState.KMER);
        stateChanger.add(KmersFromFileState.KMER, '\n', KmersFromFileState.ID);
        return stateChanger;
    }

    public static KmersFromFileStateChanger getFQinstance()
    {
        KmersFromFileStateChanger stateChanger = new KmersFromFileStateChanger(KmersFromFileState.OTHER);
        stateChanger.add(KmersFromFileState.OTHER, '@', KmersFromFileState.ID);
        stateChanger.add(KmersFromFileState.KMER, '@', KmersFromFileState.ID);
        stateChanger.add(KmersFromFileState.ID, '\n', KmersFromFileState.KMER);
        stateChanger.add(KmersFromFileState.KMER, '+',KmersFromFileState.OTHER);
        return stateChanger;
    }

    public enum KmersFromFileState
    {
        ID,
        KMER,
        OTHER
    }
}
