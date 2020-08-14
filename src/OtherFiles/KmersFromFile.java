package OtherFiles;

import Exceptions.InvalidBaseException;
import Kmers.Kmer;
import Kmers.KmerWithData;
import Kmers.KmerWithDataStreamWrapper;
import Reads.ReadIDMapping;
import Reads.ReadPos;

import java.io.*;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KmersFromFile<D>
{
    public KmersFromFile(KmersFromFileStateChanger stateChanger, int minK, int maxK,
                         BiFunction<String,Short,D> mapper)
    {
        this.stateChanger = stateChanger;
        this.minK = minK;
        this.maxK = maxK;
        this.mapper = mapper;
    }

//    public Stream<KmerWithData<D>> streamFromFile(BufferedReader input) throws IOException
    public KmerWithDataStreamWrapper<D> streamFromFile(BufferedReader input) throws IOException
    {
        KmersFromFileSpliterator<D>  spliterator = new KmersFromFileSpliterator<D>(input, mapper, minK, maxK, stateChanger);
//        return StreamSupport.stream(spliterator,false);
        return new KmerWithDataStreamWrapper<>(StreamSupport.stream(spliterator,false),minK,maxK);
    }

    private KmersFromFileStateChanger stateChanger;
    private int minK;
    private int maxK;
    private BiFunction<String,Short,D> mapper;

    public static KmersFromFile<Integer> getFQtoRefDBInstance(int minK, int maxK)
    {
        return new KmersFromFile<>(KmersFromFileStateChanger.getFAinstance(),minK,maxK,(s, i) -> Integer.parseInt(s));
    }

    public static KmersFromFile<ReadPos> getFAtoReadDBInstance(int minK, int maxK, ReadIDMapping readMap)
    {
        return new KmersFromFile<>(KmersFromFileStateChanger.getFQinstance(), minK, maxK, (s,i) -> new ReadPos(readMap.getIntID(s), i));
    }


    public class KmersFromFileSpliterator<D> implements Spliterator<KmerWithData<D>>
    {
        public KmersFromFileSpliterator(BufferedReader input, BiFunction<String,Short,D> mapper, int minK, int maxK,
                                        KmersFromFileStateChanger stateChanger) throws IOException
        {
            this.mapper = mapper;
            this.maxK = maxK;
            this.minK = minK;
            this.in = input;
            kmerbytes = new byte[maxK];
            state = stateChanger.startState();
            this.stateChanger = stateChanger;
            id = new StringBuilder();

            ending = false;
            endFile = false;
        }

        @Override
        public boolean tryAdvance(Consumer<? super KmerWithData<D>> consumer)
        {
            try
            {
                do
                {
                    if (ending)
                    {
                        if (curK < minK)
                        {
                            ending = false;
                            kwd = null;
                        }
                        else
                        {
                            byte[] newbytes = new byte[curK];
                            System.arraycopy(kmerbytes, maxK-curK, newbytes, 0, curK);
                            try
                            {
                                kwd = new KmerWithData<D>(new Kmer(newbytes), mapper.apply(oldid.toString(), (short) (pos - curK)));
                            }
                            catch (InvalidBaseException ex)
                            {
                                //Shouldn't get here
                            }
                            curK--;
                        }
                    }
                    else
                    {

                        int c = in.read();
                        if (c == -1)
                        {
//                            throw new EOFException();
                            ending = true;
                            oldid = id.toString();
                            curK = Math.min(pos-1,maxK - 1);
                            endFile = true;
                            break;
                        }
                        KmersFromFileStateChanger.KmersFromFileState newState = stateChanger.get(state, c);

                        if (newState != null)
                        {
                            if ((state == KmersFromFileStateChanger.KmersFromFileState.KMER) && (pos >= minK))
                            {
                                oldid = id.toString();
                                ending = true;
                                curK = Math.min(pos,maxK - 1);
                            }
                            state = newState;
                            switch (state)
                            {
                                case KMER:
                                    pos = 0;
                                    break;
                                case ID:
                                    id = new StringBuilder();
                                    kwd = null;
                                    break;
                                case OTHER:
                                    kwd = null;
                                    break;
                            }
                        }
                        else
                        {
                            switch (state)
                            {
                                case KMER:
                                    if (c > 32) // This is greater than space, nothing to do with kmer size...
                                    {
                                        pos++;
                                        System.arraycopy(kmerbytes, 1, kmerbytes, 0, maxK - 1);
                                        kmerbytes[maxK - 1] = (byte) c;
                                        if (pos >= maxK)
                                        {
                                            try
                                            {
                                                byte[] newbytes = new byte[maxK];
                                                System.arraycopy(kmerbytes, 0, newbytes, 0, maxK);
                                                kwd = new KmerWithData<D>(new Kmer(newbytes), mapper.apply(id.toString(), (short) (pos - maxK)));
                                            }
                                            catch (InvalidBaseException ex)
                                            {
                                                kwd = null;
                                                if (pos > minK)
                                                {
                                                    ending = true;
                                                    oldid = id.toString();
                                                    curK = Math.min(pos-1,maxK - 1);
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    else
                                    {
                                        kwd = null;
                                    }
                                    break;
                                case ID:
                                    id.appendCodePoint(c);
                                    break;
                            }
                        }
                    }
                }
                while ((kwd == null) && (!endFile || (endFile || ending)));
                if (kwd == null)
                {
                    return false;
                }
                else
                {
                    consumer.accept(kwd);
                    return true;
                }
            }
            catch (IOException ex)
            {
//                return false;
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public long estimateSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public Spliterator<KmerWithData<D>> trySplit()
        {
            return null;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.IMMUTABLE | Spliterator.ORDERED;
        }

        private boolean ending;
        private boolean endFile;
        private int curK;
        private String oldid;

        private BiFunction<String,Short,D> mapper;
        private int minK;
        private int maxK;
        private int pos;
        private BufferedReader in;
        private StringBuilder id;
        private byte[] kmerbytes;
        private KmerWithData<D> kwd;
        private KmersFromFileStateChanger.KmersFromFileState state;
        private KmersFromFileStateChanger stateChanger;
    }
}
