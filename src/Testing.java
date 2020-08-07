import CountMaps.CountMap;
import CountMaps.TreeCountMap;
import Files.ZippedIndexedInputFile;
import KmerFiles.CompressedKmerFile;
import KmerFiles.CountCompressor;
import Kmers.*;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

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
//        ZippedIndexedInputFile rf = new ZippedIndexedInputFile(new File("reads/test.db.gz"),
//                new File("reads/test.ind.gz"));
//
//        ReadPosSetCompressor ecompressor = new ReadPosSetCompressor();
//        CompressedKmerFile<Set<ReadPos>> crf = new CompressedKmerFile<>(rf, ecompressor);
//
//        ZippedIndexedInputFile df = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz"));
//        CountCompressor dcompressor = new CountCompressor();
//        CompressedKmerFile<TreeCountMap<Integer>> cdf = new CompressedKmerFile<>(df, dcompressor);
//
//
////        long c = StreamUtils.matchTwoStreams(crf.allKmers(), cdf.allKmers(), (kwd1, kwd2) -> kwd1.toString() + "\t" + kwd2.toString(),
////                (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()))
////                .count();
//
//        for (int i = 0; i < 100; i++)
//        {
//            long c =
//                    StreamUtils.matchTwoStreams(crf.kmers(0).filter(k -> k.getKmer().length() == 32), cdf.kmers(0), (kwd1, kwd2) -> kwd1.toString() + "\t" + kwd2.toString(),
//                            (kwd1, kwd2) -> kwd1.getKmer().compareTo(kwd2.getKmer()))
//                            .count();
//            //                .forEach(ci -> System.out.println(ci));
//
////            System.out.println(c);
//        }
//
//
//        /********************
//         * Inexact testing
//         */
//
//        ZippedIndexedInputFile rf2 = new ZippedIndexedInputFile(new File("reads/test.db.gz"),
//                new File("reads/test.ind.gz"));
//
//        ReadPosSetCompressor ecompressor2 = new ReadPosSetCompressor();
//        CompressedKmerFile<Set<ReadPos>> crf2 = new CompressedKmerFile<>(rf2, ecompressor2);
//
//        ZippedIndexedInputFile df2 = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
//                new File("medium/norway_cr.ind.gz"));
//        CountCompressor dcompressor2 = new CountCompressor();
//        CompressedKmerFile<TreeCountMap<Integer>> cdf2 = new CompressedKmerFile<>(df2, dcompressor2);
//        List<KmerFile<TreeCountMap<Integer>>> files = new ArrayList<>(1);
//        files.add(cdf2);
//        DB<TreeCountMap<Integer>> db = new DB<>(files, 6, (d1, d2) -> {d1.addAll(d2); return d1;});
//
//        System.out.println();
//        System.out.println(sdf.format(new Date()));
//        System.out.println();
//
//        for (int i = 0; i < 100; i++)
//        {
//            long c2 =
//                    db.getNearestKmers2(crf.kmers(0).filter(k -> k.getKmer().length() == 32), 0, false).filter(ci -> !ci.getMatchedKmers().isEmpty())
//                            .count();
//            //                .forEach(ci -> System.out.println(ci));
////            System.out.println(c2);
//        }

//        List<Kmer> l = crf.kmers(0).map(kwd -> kwd.getKmer()).collect(Collectors.toList());
//        db.getNearestKmers(l,0,false).entrySet().stream().forEach(e -> System.out.println(e.getKey() + "\t" + e.getValue()));
//        db.getNearestKmers(l,0,false);

//        Root<TreeCountMap<Integer>> r = new Root<>(32,24,(d1, d2) -> {d1.addAll(d2); return d1;});
//        cdf.kmers(0).forEach(k -> r.addKmer(k));
//        l.stream().map(k -> r.closestKmers(k,0,false)).filter(ci -> !ci.getKmers().isEmpty()).forEach(ci -> System.out.println(ci));

//        crf.kmers(0).map(kwd -> r.closestKmers2(kwd,0,false)).filter(ci -> !ci.getMatchedKmers().isEmpty()).forEach(ci -> System.out.println(ci));



        /*******
         * Playing with limiting to certain lengths
         */


        ZippedIndexedInputFile f = new ZippedIndexedInputFile(new File("medium/norway_cr.db.gz"),
                new File("medium/norway_cr.ind.gz"));
        CountCompressor compressor = new CountCompressor();
        CompressedKmerFile<TreeCountMap<Integer>> dbf = new CompressedKmerFile<>(f, compressor);

        dbf.allRestrictedKmers(28,32,(kwd1,kwd2) -> {kwd1.getData().addAll(kwd2.getData()); return kwd1;}).limit(200).forEach(kwd -> System.out.println(kwd));



        System.out.println(sdf.format(new Date()));
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