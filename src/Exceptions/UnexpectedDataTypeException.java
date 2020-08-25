package Exceptions;

import java.io.File;
import java.io.IOException;

public class UnexpectedDataTypeException extends IOException
{
    public UnexpectedDataTypeException(File f)
    {
        super("Unexpected data type in " + f.toString());
        this.f = f;
    }

    public File getFile()
    {
        return f;
    }

    private File f;
}
