package Database;

public class Node<D>
{
    protected Node(Node parent, byte[] seq)
    {
        this.parent = parent;
        this.seq = seq;
    }

    private Node parent;
    protected byte[] seq;
    protected Node[] children = new Node[4];
    protected D data;
}
