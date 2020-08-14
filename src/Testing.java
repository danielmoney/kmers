import Compression.IntCompressor;
import CountMaps.CountMap;
import CountMaps.TreeCountMap;
import Database.DB;
import IndexedFiles.ZippedIndexedInputFile;
import KmerFiles.CountCompressor;
import KmerFiles.KmerFile;
import Kmers.*;
import Reads.ReadPos;
import Reads.ReadPosSetCompressor;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Testing
{
    public static void main(String[] args) throws Exception
    {
        System.out.println(sdf.format(new Date()));

//        KmersFromFileStateChanger stateChanger = new KmersFromFileStateChanger(KmersFromFileState.ID);
//        stateChanger.add(KmersFromFileState.ID, '\t', KmersFromFileState.KMER);
//        stateChanger.add(KmersFromFileState.ID, ' ', KmersFromFileState.KMER);
//        stateChanger.add(KmersFromFileState.KMER, '\n', KmersFromFileState.ID);

//        KmersFromFile<String> kf = new KmersFromFile<String>(new File("arctic_mammals.gz"), stateChanger, 32, 24, (s, i) -> s);
//
//        DBCreator create = new DBCreator(new File("arctic_mammals.db.gz"),
//                new File("arctic_mammals.ind.gz"), 6, 32);

//        KmersFromFile<String> kf = new KmersFromFile<String>(new File("small/test.dat"), stateChanger, 32, 24, (s, i) -> s);
//
//        DBCreator create = DBCreator.getZippedInstance(new File("small/test.db.gz"),
//                new File("small/test.ind.gz"), 6, 32);

//        DBCreator create = DBCreator.getStandardInstance(new File("test.db"),
//                new File("test.ind"), 6, 32);

//        DBCreator create = DBCreator.getStandardZippedInstance(new File("test.db.gz"),
//                new File("test.ind.gz"), 6, 32);

//        KmersFromFile<String> kf = new KmersFromFile<String>(new File("medium/norway_cr.gz"), stateChanger, 32, 24, (s, i) -> s);
//
//        DBCreator create = DBCreator.getZippedInstance(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz"), 6, 32, 400, 9);
//
//        kf.streamFromFile().forEach(create);
//
////        create.accept(new KmerWithData(new Kmer("GCCTGAGTCTCTTCACTACCTCGCTGCCCTCC"),"Testing"));
//
//        create.create();
//
//        create.close();

//        List<DBFile<CountMap<Integer>>> files = new ArrayList<>(1);
//        files.add(new CompressedDBFile(new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz")), new CountCompressor()));
////
//        DB<CountMap<Integer>> db = new DB<>(files, 6, (d1, d2) -> {d1.addAll(d2); return d1;});
////
//        List<Kmer> test = new ArrayList(1);
////        Kmer t = new Kmer("GCCTGAGTCTCTTCACCACCTCGCTGCCCTCC"); // 0
////        Kmer t = new Kmer("GCCTGAGTCTCTTCACCACCTCGCTGCCCTCG"); // 1
////        Kmer t = new Kmer("GCCTGAGTCTCTTCACTACCTCGCTGCCCTCC"); // 1
//        Kmer t = new Kmer("tctcccggataagcttctcggccaggtggtct");
//        test.add(t);
//        Map<Kmer,ClosestInfo<CountMap<Integer>>> closest = db.getNearestKmers(test,4,false);
//        ClosestInfo<CountMap<Integer>> c = closest.get(t);
//
//        System.out.println(c.getDist());
//        for (Map.Entry<KmerWithData<CountMap<Integer>>,Integer> e: c.getKmers().entrySet())
//        {
//            System.out.println(e.getValue() + "\t" + e.getKey());
//        }
//        System.out.println(t);
//
//        System.out.println();

//        db.test();

//        Kmer k = new Kmer("-----ACTG");
//        System.out.println(k);

//        Kmer t = new Kmer("ccaactcttacttcgttattatattgttcgat");
//        test.add(t);
//
//        Map<Kmer,ClosestInfo<String>> closest = db.getNearestKmers(test,4,false);
//        ClosestInfo<String> c = closest.get(t);
//
//        System.out.println(c.getDist());
//        for (Map.Entry<KmerWithData<String>,Integer> e: c.getKmers().entrySet())
//        {
//            System.out.println(e.getKey() + "\t" + e.getValue());
//        }

//
//
//
//        Kmer t = new Kmer("GCCTGAGTCTCTTCACCACCTCGCTGCCCTCC");
//        System.out.println(Arrays.toString(t.compressedBytes()));
//
//        Kmer to = Kmer.createFromCompressed(t.compressedBytes());
//        System.out.println(t);
//        System.out.println(to);
//
//        System.out.println();
//
//        Kmer t2 = new Kmer("GCCTGAGTCTCTTCACCACCTCTGCCCTCC");
//        System.out.println(Arrays.toString(t2.compressedBytes()));
//        Kmer t2o = Kmer.createFromCompressed(t2.compressedBytes());
//        System.out.println(t2);
//        System.out.println(t2o);
//
//        System.out.println();
//
//        KmerWithData<Integer> dt = new KmerWithData<Integer>(new Kmer("GCCTGAGTCTCTTCACCACCTCGCTGCCCTCC"), 12345);
//        byte[] cd = dt.compressedBytes(new IntCompressor());
//        System.out.println(Arrays.toString(cd));
//        KmerWithData<Integer> dto = KmerWithData.createFromCompressed(cd,new IntCompressor());
//        System.out.println(dt);
//        System.out.println(dto);

//        ZippedIndexedInputFile f = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz"));

//        byte[] data = f.data(0);
//
//        CountCompressor compressor = new CountCompressor();
//
//        CompressedDBSpliterator spliterator = new CompressedDBSpliterator(data,compressor);
//        StreamSupport.stream(spliterator,false).limit(100).forEach(kwd -> printkwd(kwd));

//        List<In>
//        DB db = new DB()

//        ZippedIndexedInputFile f = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz"));
//        CountCompressor compressor = new CountCompressor();
//        CompressedKmerFile<TreeCountMap<Integer>> dbf = new CompressedKmerFile<>(f, compressor);
//
//        StreamUtils.groupedStream(dbf.kmers(0).limit(1000), (k1, k2) -> k1.getKmer().key(8) == k2.getKmer().key(8), Collectors.toList()).
//                forEach(l -> System.out.println("\n" + l));
//
//        System.out.println(sdf.format(new Date()));


//        /*****************
//         * Exact match code
//         *****************/
//
//        ZippedIndexedInputFile rf = new ZippedIndexedInputFile(new File("reads/test.db.gz"));
//
//        ReadPosSetCompressor ecompressor = new ReadPosSetCompressor();
//        CompressedKmerFile<Set<ReadPos>> crf = new CompressedKmerFile<>(rf, ecompressor);
//
//        ZippedIndexedInputFile df = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"));
//        CountCompressor dcompressor = new CountCompressor();
//        CompressedKmerFile<TreeCountMap<Integer>> cdf = new CompressedKmerFile<>(df, dcompressor);


//        long c = StreamUtils.matchTwoStreams(crf.allKmers(), cdf.allKmers(), (kwd1, kwd2) -> kwd1.toString() + "\t" + kwd2.toString(),
//                (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()))
//                .count();

//        for (int i = 0; i < 100; i++)
//        {
//            long c =
//                    StreamUtils.matchTwoStreams(crf.kmers(0).stream().filter(k -> k.getKmer().length() == 32), cdf.kmers(0).stream(), (kwd1, kwd2) -> kwd1.toString() + "\t" + kwd2.toString(),
//                            (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()))
//                            .count();
            //                .forEach(ci -> System.out.println(ci));

//        Stream<KmerWithData<Set<ReadPos>>> rstream = IntStream.range(0,4096).mapToObj(i ->
//                crf.restrictedKmers(i,32,32,(kwd1,kwd2) -> {kwd1.getData().addAll(kwd2.getData()); return kwd1;})
//                        .stream()).flatMap(s -> s);
//        Stream<KmerWithData<TreeCountMap<Integer>>> dstream = IntStream.range(0,4096).mapToObj(i ->
//                cdf.restrictedKmers(i,32,32,(kwd1,kwd2) -> {kwd1.getData().addAll(kwd2.getData());return kwd2;})
//                        .stream()).flatMap(s -> s);
//
//        long c = StreamUtils.matchTwoStreams(rstream,dstream,
//                        (kwd1, kwd2) -> kwd1.toString() + "\t" + kwd2.toString(),
//                        (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()))
//                .count();
//
//            System.out.println(c);

//        }
//
//
        /********************
         * Inexact testing
         */

//        ZippedIndexedInputFile<Integer> rf2 = new ZippedIndexedInputFile<>(new File("reads/test.db.gz"), new IntCompressor());
//
//        ReadPosSetCompressor ecompressor2 = new ReadPosSetCompressor();
//        KmerFile<Set<ReadPos>> crf2 = new KmerFile<>(rf2, ecompressor2);
//
//        ZippedIndexedInputFile<Integer> df2 = new ZippedIndexedInputFile<>(new File("medium/norway_cr.db.gz"), new IntCompressor());
//        CountCompressor dcompressor2 = new CountCompressor();
//        KmerFile<TreeCountMap<Integer>> cdf2 = new KmerFile<>(df2, dcompressor2);
//        List<KmerFile<TreeCountMap<Integer>>> files = new ArrayList<>(1);
//        files.add(cdf2);
//        DB<TreeCountMap<Integer>> db = new DB<>(files, (d1, d2) -> {d1.addAll(d2); return d1;});
//
//        System.out.println(sdf.format(new Date()));
////
//////        for (int i = 0; i < 100; i++)
//////        {
////            long c2 =
////                    db.getNearestKmers(crf.kmers(0).stream().filter(k -> k.getKmer().length() == 32), 0, false).filter(ci -> !ci.getMatchedKmers().isEmpty())
////                            .count();
//            //                .forEach(ci -> System.out.println(ci));
//
//        KmerWithDataStreamWrapper<Set<ReadPos>> rstream2 = new KmerWithDataStreamWrapper<>(
//                IntStream.range(0,4).mapToObj(i ->
//                crf2.restrictedKmers(i,32,32,(kwd1,kwd2) -> {kwd1.getData().addAll(kwd2.getData()); return kwd1;})).flatMap(s -> s.stream()),
//                32,32);
//
//        long c2 = db.getNearestKmers(rstream2,0,true).filter(ci -> !ci.getMatchedKmers().isEmpty()).count();
//
//            System.out.println(c2);

        System.out.println(sdf.format(new Date()));

////        }

        /************
         * Not sure!
         */

//        List<KmerWithData<Set<ReadPos>>> l = crf.kmers(0).stream().collect(Collectors.toList());
//        db.getNearestKmers(l,0,false).forEach(ci -> System.out.println(ci));
//        db.getNearestKmers(l,0,false);
//
//        Root<TreeCountMap<Integer>> r = new Root<>(32,24,(d1, d2) -> {d1.addAll(d2); return d1;});
//        cdf.kmers(0).stream().forEach(k -> r.addKmer(k));
//        l.stream().map(k -> r.closestKmers(k,0,false)).filter(ci -> !ci.getMatchedKmers().isEmpty()).forEach(ci -> System.out.println(ci));
//
//        crf.kmers(0).stream().map(kwd -> r.closestKmers(kwd,0,false)).filter(ci -> !ci.getMatchedKmers().isEmpty()).forEach(ci -> System.out.println(ci));



        /*******
         * Playing with limiting to certain lengths
         */


//        ZippedIndexedInputFile f = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz"));
//        CountCompressor compressor = new CountCompressor();
//        CompressedKmerFile<TreeCountMap<Integer>> dbf = new CompressedKmerFile<>(f, compressor);
//
//        dbf.allRestrictedKmers(28,32,(kwd1,kwd2) -> {kwd1.getData().addAll(kwd2.getData()); return kwd1;}).stream().limit(200).forEach(kwd -> System.out.println(kwd));


        /****************
         * CLI bits
         */

//        Options options = new Options();
//        options.addOption(Option.builder("d").hasArgs().build());
//
//        String[] test = {"-d", "file1a", "-d", "file1b"};
//        CommandLineParser parser = new DefaultParser();
//        CommandLine line = parser.parse(options,test);
//
//        String[] v = line.getOptionValues('d');
//        System.out.println(Arrays.toString(v));


    }

    private static void printkwd(KmerWithData<CountMap<Integer>> kwd)
    {
        System.out.print(kwd.getKmer());
        System.out.print("\t");
        for (Map.Entry<Integer,Long> e: kwd.getData().entrySet())
        {
            System.out.print(e.getKey() + ":" + e.getValue() + " ");
        }
        System.out.println();
    }

    private static CountMap<Integer> mergeCountMap(CountMap<Integer> d1, CountMap<Integer> d2)
    {
        d1.addAll(d2);
        return d1;
    }

    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss\t");
}