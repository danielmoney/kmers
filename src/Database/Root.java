package Database;

import DataTypes.DataType;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Kmers.KmerStream;

import java.util.*;
import java.util.function.BiConsumer;

public class Root<D>
{
    Root(int kmerLength, int shortest, DataType<?,D> dataType)
    {
        size = 0;
        root = null;
        this.kmerLength = kmerLength;
        this.shortest = shortest;
        merge = dataType.getMerger();
    }

    void addKmers(KmerStream<D> stream)
    {
        // Checking the stream makes sense to add should have been done by the DB class

        stream.forEach(k -> addKmer(k));
    }

    private void addKmer(KmerWithData<D> kmer)
    {
        boolean already = false;
        if (root == null)
        {
            root = new Node<D>(null, kmer.getKmer().getRawBytes());
            root.data = kmer.getData();
        }
        else
        {
            byte[] bytes = kmer.getKmer().getRawBytes();
            int s = 0;
            Node<D> n = root;
            while (s != kmer.getKmer().length())
            {
                int d = firstDiff(bytes, s, n.seq);
                if (d == n.seq.length)
                {
                    s += n.seq.length;
                    if (s >= shortest)
                    {
                        merge.accept(n.data, kmer.getData());
                    }
                    if (s == kmer.getKmer().length())
                    {
                        //EXACT MATCH
                        already = true;
                    }
                    else
                    {
                        if (n.children[bytes[s]] != null)
                        {
                            n = n.children[bytes[s]];
                        }
                        else
                        {
                            Node<D> nn = new Node<D>(n,Arrays.copyOfRange(bytes,s,bytes.length));
                            nn.data = kmer.getData();
                            n.children[bytes[s]] = nn;
                            s = kmer.getKmer().length();
                        }
                    }
                }
                else
                {


                    byte b1 = n.seq[d];
                    byte[] s1 = Arrays.copyOfRange(n.seq,d,n.seq.length);
                    Node<D> n1 = new Node<D>(n,s1);
                    n1.children = n.children;
                    n1.data = n.data;

                    n.children = new Node[4];
                    n.children[b1] = n1;

                    if (s+d < bytes.length)
                    {
                        byte b2 = bytes[s + d];
                        byte[] s2 = Arrays.copyOfRange(bytes, s + d, bytes.length);
                        Node<D> n2 = new Node<D>(n, s2);
                        n2.data = kmer.getData();
                        n.children[b2] = n2;
                    }

                    n.seq = Arrays.copyOfRange(n.seq,0,d);
                    if (s + d < shortest)
                    {
                        n.data = null;
                    }
                    else
                    {
                        merge.accept(n.data,kmer.getData());
                    }

                    s = kmer.getKmer().length();
                }
            }

        }
        if (!already)
        {
            size++;
        }
    }

    <S> ClosestInfo<S,D> closestKmers(KmerWithData<S> searchKmer, int maxdiff, boolean just)
    {
        Map<KmerWithData<D>,Integer> best = new HashMap<>();
        int testDist = maxdiff;
        int bestDist = maxdiff;

        Kmer target = searchKmer.getKmer();

        LinkedList<ClosestNode<D>> process = new LinkedList<>();
        if (root != null)
        {
            process.add(new ClosestNode<D>(root, 0, new byte[0]));
        }

        byte[] tbytes = target.getRawBytes();

        while (!process.isEmpty())
        {
            ClosestNode<D> cn = process.pollFirst();
            int curdist = cn.prevdist + numDiff(tbytes,cn.prevseq.length,cn.n.seq);
            if (curdist <= testDist)
            {
                byte[] currentseq = cn.prevseq;
                byte[] newseq = new byte[Math.min(currentseq.length + cn.n.seq.length,target.length())];
                System.arraycopy(currentseq,0,newseq,0,currentseq.length);
                System.arraycopy(cn.n.seq,0,newseq,currentseq.length,Math.min(cn.n.seq.length,target.length()-currentseq.length));

                if ((cn.prevseq.length + cn.n.seq.length) >= target.length())
                {
                    if ((curdist < testDist) && just)
                    {
                        testDist = curdist;
                        best = new HashMap<>();
                    }
                    bestDist = Math.min(curdist,bestDist);
                    /************
                     * Need to check it doesn't already contain the rc kmer
                     */
                    best.put(new KmerWithData<D>(Kmer.createUnchecked(newseq), cn.n.data),curdist);
//                    System.out.println(best + "\t" + testDist);
                }
                else
                {
                    for (int i = 3; i >= 0; i--)
                    {
                        if (cn.n.children[i] != null)
                        {
                            process.addFirst(new ClosestNode<D>(cn.n.children[i], curdist, Arrays.copyOf(newseq, newseq.length)));
                        }
                    }
                }
            }
        }

        return new ClosestInfo<S,D>(searchKmer,best,bestDist);
    }

    private class ClosestNode<D>
    {
        private ClosestNode(Node<D> n, int prevdist, byte[] prevseq)
        {
            this.n = n;
            this.prevdist = prevdist;
            this.prevseq = prevseq;
        }

        private Node<D> n;
        private int prevdist;
        private byte[] prevseq;
    }

    private int numDiff(byte[] seq, int ss, byte[] nodeseq)
    {
        int c = 0;
        for (int i = 0; i < Math.min(nodeseq.length,seq.length-ss); i++)
        {
            if (seq[i+ss] != nodeseq[i])
            {
                c++;
            }
        }
        return c;
    }


    private int firstDiff(byte[] seq, int ss, byte[] nodeseq)
    {
        int i;
        for (i = 0; i < Math.min(nodeseq.length,seq.length-ss); i++)
        {
            if (seq[i+ss] != nodeseq[i])
            {
                return i;
            }
        }
        return i;
    }

    private class IteratorNode<D>
    {
        private IteratorNode(Node<D> n, byte[] prevseq)
        {
            this.n = n;
            this.prevseq = prevseq;
        }

        private Node<D> n;
        private byte[] prevseq;
    }

    private int size;
    private Node<D> root;
    private BiConsumer<D,D> merge;
    private int kmerLength;
    private int shortest;
}
