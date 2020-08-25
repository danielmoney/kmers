package Database;

import Compression.Compressor;
import DataTypes.DataType;
import DataTypes.MergeableDataType;
import Kmers.KmerWithData;
import Kmers.KmerWithDataDatatType;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

public class ClosestInfoDataType<D> implements MergeableDataType<ClosestInfo<D>>
{
    public ClosestInfoDataType(DataType<D> dCompressor)
    {
        kCompressor = new KmerWithDataDatatType<>(dCompressor);
    }

    public ClosestInfo<D> decompress(ByteBuffer bb)
    {
        ClosestInfo<D> closest = new ClosestInfo<>();

        int num = bb.getInt();

        for (int i = 0; i < num; i++)
        {
            closest.add(new ClosestInfo.CI<>(kCompressor.decompress(bb), bb.get()));
        }

        return closest;
    }

    public ClosestInfo<D> decompress(DataInput input) throws IOException
    {
        ClosestInfo<D> closest = new ClosestInfo<>();

        int num = input.readInt();

        for (int i = 0; i < num; i++)
        {
            closest.add(new ClosestInfo.CI<>(kCompressor.decompress(input),input.readByte()));
        }

        return closest;
    }

    public byte[] compress(ClosestInfo<D> ci)
    {
        ByteBuffer mbb = ByteBuffer.allocate(4);
        mbb.putInt(ci.getMatchedKmers().size());

        int c = 0;
        int s = 4;
        byte[][] temp = new byte[ci.getMatchedKmers().size()][];
        for (ClosestInfo.CI<D> cc: ci.getMatchedKmers())
        {
            byte[] k = kCompressor.compress(cc.getKWD());
            byte[] t = new byte[temp.length+1];
            System.arraycopy(k,0,t,0,temp.length);
            t[temp.length] = cc.getDist();
            temp[c] = t;
            s += temp[c].length;
            c++;
        }

        ByteBuffer bb = ByteBuffer.allocate(s);
        bb.put(mbb);
        for (int i = 0; i < temp.length; i++)
        {
            bb.put(temp[i]);
        }

        return bb.array();
    }

    public String toString(ClosestInfo<D> ci)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(ci.getMinDist());
        for (ClosestInfo.CI<D> c: ci.getMatchedKmers())
        {
            sb.append(" | ");
            sb.append(c.getDist());
            sb.append("\t");
            sb.append(kCompressor.toString(c.getKWD()));
        }
        return sb.toString();
    }

    public ClosestInfo<D> fromString(String s)
    {
        String[] parts = s.split(" | ");
        ClosestInfo<D> closest = new ClosestInfo<>();
        for (int i = 1; i < parts.length; i++)
        {
            String[] p2 = s.split("\t",2);
            closest.add(new ClosestInfo.CI<D>(kCompressor.fromString(p2[1]), Byte.parseByte(p2[0])));
        }
        return closest;
    }

    public int[] getID()
    {
        //return 8192 + kCompressor.getID();
        int[] childid = kCompressor.getID();
        int[] id = new int[childid.length+1];
        id[0] = 2049;
        System.arraycopy(childid,0,id,1,childid.length);
        return id;
    }

    public BiConsumer<ClosestInfo<D>, ClosestInfo<D>> getMerger()
    {
        return (ci1,ci2) -> ci1.addAll(ci2);
    }

    private Compressor<KmerWithData<D>> kCompressor;
}
