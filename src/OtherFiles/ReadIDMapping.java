package OtherFiles;

import java.io.*;

public class ReadIDMapping
{
    public ReadIDMapping(PrintWriter out) throws IOException
    {
        this.out = out;
        curReadS = "";
        curReadI = -1;
    }

    public int geNext(String id)
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
