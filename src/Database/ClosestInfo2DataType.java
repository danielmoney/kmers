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

public class ClosestInfo2DataType<D> implements MergeableDataType<ClosestInfo2<D>>
{
    public ClosestInfo2DataType(DataType<D> dCompressor)
    {
        kCompressor = new KmerWithDataDatatType<>(dCompressor);
    }

    public ClosestInfo2<D> decompress(ByteBuffer bb)
    {
        ClosestInfo2<D> closest = new ClosestInfo2<>();

        int num = bb.getInt();

        for (int i = 0; i < num; i++)
        {
            closest.add(new ClosestInfo2.CI<>(kCompressor.decompress(bb), bb.get()));
        }

        return closest;
    }

    public ClosestInfo2<D> decompress(DataInput input) throws IOException
    {
        ClosestInfo2<D> closest = new ClosestInfo2<>();

        int num = input.readInt();

        for (int i = 0; i < num; i++)
        {
            closest.add(new ClosestInfo2.CI<>(kCompressor.decompress(input),input.readByte()));
        }

        return closest;
    }

    public byte[] compress(ClosestInfo2<D> ci)
    {
        ByteBuffer mbb = ByteBuffer.allocate(4);
        mbb.putInt(ci.getMatchedKmers().size());

        int c = 0;
        int s = 4;
        byte[][] temp = new byte[ci.getMatchedKmers().size()][];
        for (ClosestInfo2.CI<D> cc: ci.getMatchedKmers())
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

    public String toString(ClosestInfo2<D> ci)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(ci.getMinDist());
        for (ClosestInfo2.CI<D> c: ci.getMatchedKmers())
        {
            sb.append(" | ");
            sb.append(c.getDist());
            sb.append("\t");
            sb.append(kCompressor.toString(c.getKWD()));
        }
        return sb.toString();
    }

    public ClosestInfo2<D> fromString(String s)
    {
        String[] parts = s.split(" | ");
        ClosestInfo2<D> closest = new ClosestInfo2<>();
        for (int i = 1; i < parts.length; i++)
        {
            String[] p2 = s.split("\t",2);
            closest.add(new ClosestInfo2.CI<D>(kCompressor.fromString(p2[1]), Byte.parseByte(p2[0])));
        }
        return closest;
    }

    public int getID()
    {
        return 8192 + kCompressor.getID();
    }

    public BiConsumer<ClosestInfo2<D>, ClosestInfo2<D>> getMerger()
    {
        return (ci1,ci2) -> ci1.addAll(ci2);
    }

    private Compressor<KmerWithData<D>> kCompressor;
}
