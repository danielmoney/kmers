package Exceptions;

public class UnknownTaxaException extends Exception
{
    public UnknownTaxaException(int id)
    {
        super(id + " is not known");
        this.id = id;
    }

    protected UnknownTaxaException(String s, int id)
    {
        super(s);
        this.id = id;
    }

    public int getId()
    {
        return id;
    }

    private int id;
}
