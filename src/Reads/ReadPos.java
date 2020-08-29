package Reads;

public class ReadPos implements Comparable<ReadPos>
{
    public ReadPos(int read, short pos)
    {
        this.read = read;
        this.pos = pos;
    }

    public int getRead()
    {
        return read;
    }

    public short getPos()
    {
        return pos;
    }

    public String toString()
    {
        return read + ":" + pos;
    }

    public int compareTo(ReadPos other)
    {
        int c = Integer.compare(read,other.read);
        if (c != 0)
        {
            return c;
        }
        return Short.compare(pos,other.pos);
    }

    private int read;
    private short pos;
}
