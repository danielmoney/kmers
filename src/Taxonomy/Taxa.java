package Taxonomy;

import java.util.Set;
import java.util.TreeSet;

public class Taxa implements Comparable<Taxa>
{
    public Taxa(int taxid)
    {
        this(taxid,null);
    }

    public Taxa(int taxid, String name)
    {
        this.taxid = taxid;
        this.name = name;
    }

    public Taxa(int id, int parentid, String rank, int division)
    {
        this.taxid = id;
        this.parentid = parentid;
        this.rank = rank;
        this.childids = new TreeSet<>();
        this.division = division;
    }

    public int getTaxid()
    {
        return taxid;
    }

    public void addChild(int child)
    {
        childids.add(child);
    }

    public void addName(String name)
    {
        this.name = name;
    }

    public int getID()
    {
        return taxid;
    }

    public int getParentID()
    {
        return parentid;
    }

    public int getDivision()
    {
        return division;
    }

    public String getRank()
    {
        return rank;
    }

    public Set<Integer> getChildren()
    {
        return childids;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Taxa))
        {
            return false;
        }
        return taxid == ((Taxa) o).taxid;
    }

    public String toString()
    {
        if (name != null)
        {
            return name;
        }
        return Integer.toString(taxid);
    }

    public int compareTo(Taxa t2)
    {
        return Integer.compare(taxid,t2.taxid);
    }

    public int hashCode()
    {
        return Integer.hashCode(taxid);
    }

    private int taxid;
    private String name;
    private int parentid = -1;
    private int division;
    private String rank;
    private Set<Integer> childids;
}
