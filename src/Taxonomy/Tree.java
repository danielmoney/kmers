package Taxonomy;

import Exceptions.DeletedTaxaException;
import Exceptions.UnknownTaxaException;
import Zip.ZipOrNot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Tree
{
    public Tree(File taxf) throws IOException
    {
        this(taxf,null);
    }

    public Tree(File taxf, File namef) throws IOException
    {
        taxa = new TreeMap<>();
        deleted = new TreeSet<>();

        BufferedReader in = new BufferedReader(new FileReader(taxf));

        String line = in.readLine();

        //Deal with root specially due to stupid circular parent
        String[] parts = line.split("\\t\\|\\t");
        int id = Integer.parseInt(parts[0]);
        taxa.put(id,
                new Taxa(id,
                        -1,
                        parts[2],
                        Integer.parseInt(parts[4])));

        line = in.readLine();
        while (line != null)
        {
            parts = line.split("\\t\\|\\t");
            id = Integer.parseInt(parts[0]);
            taxa.put(id,
                    new Taxa(id,
                    Integer.parseInt(parts[1]),
                    parts[2],
                    Integer.parseInt(parts[4])));
            line = in.readLine();
        }

        in.close();

        for (Taxa n: taxa.values())
        {
            if (n.getID() != 1)
            {
                taxa.get(n.getParentID()).addChild(n.getID());
            }
        }

        if (namef != null)
        {
            in = new BufferedReader(new FileReader(namef));

            line = in.readLine();
            while (line != null)
            {
                parts = line.split("\\t\\|\\t");
                String type = parts[3];
                type = type.replace("\t|","");
                if (type.equals("scientific name"))
                {
                    id = Integer.parseInt(parts[0]);
                    taxa.get(id).addName(parts[1]);
                }
                line = in.readLine();
            }

            in.close();
        }

    }

    public void addMerged(File f) throws IOException
    {
        BufferedReader in = new BufferedReader(new FileReader(f));
        String line = in.readLine();
        while (line != null)
        {
            String[] parts = line.split("\\t\\|\\t");
            int mergedTo = Integer.parseInt(parts[1].replace("\t|",""));
            taxa.put(Integer.parseInt(parts[0]),taxa.get(mergedTo));
            line = in.readLine();
        }
        in.close();
    }

    public void addDeleted(File f) throws IOException
    {
        BufferedReader in = new BufferedReader(new FileReader(f));
        String line = in.readLine();
        while (line != null)
        {
            int del = Integer.parseInt(line.replace("\t|",""));
            deleted.add(del);
            line = in.readLine();
        }
        in.close();
    }

    public Taxa getNode(int id) throws UnknownTaxaException
    {
        if (taxa.containsKey(id))
        {
            return taxa.get(id);
        }
        else
        {
            if (deleted.contains(id))
            {
                throw new DeletedTaxaException(id);
            }
            else
            {
                throw new UnknownTaxaException(id);
            }
        }
    }

    public Taxa getLCA(Collection<Taxa> taxa)
    {

        List<Integer> current = new ArrayList<>();
        for (Taxa t : taxa)
        {
            int id = t.getID();
            if (!current.isEmpty())
            {
                while (!current.contains(id))
                //CURRENT DOES NOT CONTAIN -1, HENCE INFINITE LOOP!!
                //BUT ERRR, IF EVERYTHING HAS ONE AT THE ROOT...
                //AHHH, UNKNOWN TAXA??
                {
                    try
                    {
                        id = getNode(id).getParentID();
                    }
                    catch (UnknownTaxaException ex)
                    {
                        //Horrible Hack.  Unknown taxa id should be dealt with when adding to data but for now...
                        current.add(1);
                        id = 1;
                    }
                    catch (NullPointerException ex)
                    {
                        System.err.println(id);
                        throw ex;
                    }
                }
            }
            List<Integer> newcurrent = new ArrayList<>();
            while (id != -1)
            {
                newcurrent.add(id);
                try
                {
                    id = getNode(id).getParentID();
                }
                catch (UnknownTaxaException ex)
                {
                    id = -1;
                }
            }
            current = newcurrent;
        }
            return this.taxa.get(current.get(0));
    }

    public Taxa getLCA(Taxa t1, Taxa t2)
    {
        Set<Taxa> set = new HashSet<>();
        set.add(t1);
        set.add(t2);
        return getLCA(set);
    }

    public List<Taxa> getSpeciesBelow(Taxa t)
    {
        List<Taxa> species = new ArrayList<>();
        Deque<Integer> stack = new LinkedList<Integer>();
        stack.addFirst(t.getID());
        while (!stack.isEmpty())
        {
            Taxa currentTaxa = taxa.get(stack.removeFirst());
            if (currentTaxa.getRank().equals("species"))
            {
                species.add(currentTaxa);
            }
            else
            {
                for (int c : currentTaxa.getChildren())
                {
                    stack.addFirst(c);
                }
            }
        }
        return species;
    }

    public Collection<Taxa> getNodes()
    {
        return taxa.values();
    }

    public static Tree getInstanceFromFile(File f) throws IOException
    {
        BufferedReader in = ZipOrNot.getBufferedReader(f);
        Tree t = new Tree(new File(f.getParent(), in.readLine()), new File(f.getParent(), in.readLine()));
        t.addMerged(new File(f.getParent(), in.readLine()));
        t.addDeleted(new File (f.getParent(), in.readLine()));
        return t;
    }

    private Set<Integer> deleted;
    private Map<Integer,Taxa> taxa;
}
