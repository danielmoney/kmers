package Reads;

public class ReadPos
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

    private int read;
    private short pos;
}
