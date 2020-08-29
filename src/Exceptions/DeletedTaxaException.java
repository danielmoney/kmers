package Exceptions;

public class DeletedTaxaException extends UnknownTaxaException
{
    public DeletedTaxaException(int id)
    {
        super(id + " has been deleted", id);
    }
}
