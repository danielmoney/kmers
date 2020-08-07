package Reads;

import java.io.*;
import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.zip.GZIPOutputStream;

public class ReadIDMapping
{
    public ReadIDMapping(PrintWriter out) throws IOException
    {
        this.out = out;
        curReadS = "";
        curReadI = -1;
    }

    public int getIntID(String id)
    {
        if (!id.equals(curReadS))
        {
            out.println(id);
            curReadS = id;
            curReadI ++;
        }
        return curReadI;
    }

    PrintWriter out;
    private String curReadS;
    private int curReadI;
}
