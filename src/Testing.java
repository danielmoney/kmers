import Compression.IntCompressor;
import CountMaps.CountMap;
import IndexedFiles.*;
import Kmers.*;

import java.io.File;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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


        /********************
         * Testing after major rewrite
         */


//        ReadPosSetDataType rt = new ReadPosSetDataType();
//        CountDataType dbt = new CountDataType();
//
//        ClosestInfoDataType<TreeCountMap<Integer>> cit = new ClosestInfoDataType<>(dbt);
//
//        DataPairDataType<Set<ReadPos>, ClosestInfo<TreeCountMap<Integer>>> rest = new DataPairDataType<>(rt,cit);
//
//        KmerWithDataDatatType<DataPair<Set<ReadPos>, ClosestInfo<TreeCountMap<Integer>>>> kwdt = new KmerWithDataDatatType<>(rest);
//
//
//        KmerFile<TreeCountMap<Integer>> dbf = new KmerFile<>(new File("medium/norway_cr.db.gz"), dbt);
//        DB<TreeCountMap<Integer>> db = new DB<>(dbf);
//
//        KmerFile<Set<ReadPos>> rf = new KmerFile<>(new File("reads/test.db.gz"), rt);

        /*** Exact ***/
//        int diff = 0;
//        int min = 32;

        /*** Inexact ***/
//        int diff = 1;
//        int min = 32;

        /*** Exact but slow code ***/
//        int diff = 0;
//        int min = 24;

//        KmerStream<DataPair<Set<ReadPos>, ClosestInfo<TreeCountMap<Integer>>>> resultStream =
//                db.getNearestKmers(rf.restrictedKmers(0,4096,min,32),diff,true);
////        resultStream.filter(kwd -> kwd.getData().getB().hasMatches()).forEach(kwd ->
////                System.out.println(kwdt.toString(kwd)));
//        System.out.println(resultStream.filter(kwd -> kwd.getData().getB().hasMatches()).count());
//        resultStream.close();
//

//
//        KmerWithDataDatatType<Set<ReadPos>> rpdt = new KmerWithDataDatatType<>(rt);
//        rf.allKmers().limit(100).forEach(kwd -> System.out.println(rpdt.toString(kwd)));

//        KmerWithDataDatatType<TreeCountMap<Integer>> cidt = new KmerWithDataDatatType<>(dbt);
//        db.allKmers().limit(100).forEach(kwd -> System.out.println(cidt.toString(kwd)));


        /*************************
         * Diff testing
         */

//        Kmer k1 = new Kmer("ACTGACTG");
//        Kmer k2 = new Kmer("ACTTACTT");
//
//        KmerDiff diff = new KmerDiff(k1,k2);
//        Kmer kt = diff.apply(k1);
//        System.out.println(kt);
//        System.out.println(diff);
//
//        KmerDiffDataType dt = new KmerDiffDataType();
//
//        KmerDiff diff2 = dt.fromString(dt.toString(diff));
//        System.out.println(diff2);
//
//        ByteBuffer bb = ByteBuffer.wrap(dt.compress(diff));
//        KmerDiff diff3 = dt.decompress(bb);
//        System.out.println(diff3);

//        ZippedIndexedInputFile<String> in = new ZippedIndexedInputFile<>(new File(args[0]), new StringCompressor());
////        in.indexes().stream().forEach(i -> System.out.println(i));
//        ByteBuffer input = ByteBuffer.wrap(in.data("_a"));
//        DataPairDataType<String,Sequence> stringPairDataType = new DataPairDataType<>(new StringDataType(), new SequenceDataType());
//        DataPair<String,Sequence> dp = stringPairDataType.decompress(input);
//        System.out.println(dp.getA());

//        ResultsDataType<Set<ReadPos>, TreeCountMap<Integer>> dt = ResultsDataType.getReadReferenceInstance();
//
//        (new ResultsFile<>(new File("match/test.gz"), dt)).stream().limit(10).forEach(
//                r -> System.out.println(dt.toString(r))
//        );
//

//        BufferedReader br = ZipOrNot.getBufferedReader(new File("map.dat"));
//        Map<String, Integer> map = new HashMap<>();
//        br.lines().forEach(line -> {
//            String[] parts = line.split("\t");
//            map.put(parts[0], Integer.parseInt(parts[1]));
//        });
//        System.out.println(map.keySet().stream()
//                .map(key -> key + "=" + map.get(key))
//                .collect(Collectors.joining(", ", "{", "}")));
//        KmersFromFile<Integer> kf = KmersFromFile.getFAtoRefDBInstance(24, 48, map);
//
//        kf.streamFromFile(ZipOrNot.getBufferedReader(new File("GCA_000001905.1_Loxafr3.0_genomic.fna.gz"))).limit(100).forEach(kwd -> System.out.println(kwd));

//        Kmer k = new Kmer("ACTTCA");
//
//        System.out.println(k.isOwnRC());

//        String s = "1TB";
//
//        Pattern p = Pattern.compile("([0-9\\.]+)([kMGT])?B?");
//        Matcher m = p.matcher(s);
//        System.out.println(m.matches());
//        System.out.println(m.group(1));
//        System.out.println(m.group(2));
//
//        System.out.println(SizeConvertor.fromHuman(s));

//        IndexedOutputFileSet<Integer> set = new IndexedOutputFileSet<>(f -> new StandardIndexedOutputFile<>(f, new IntCompressor(), true, 51),
//                new File("settest"));

        IndexedOutputFileSet<Integer> set = new IndexedOutputFileSet<>(f -> new ZippedIndexedOutputFile<>(f, new IntCompressor(), true, 5, 148),
                new File("settest"));

        byte[] w = new byte[8];
        Arrays.fill(w,(byte) 65);
        w[7] = '\n';

        set.write(w, 0);
        set.write(w, 0);
        set.close();

        List<IndexedInputFile<Integer>> list = new ArrayList<>(2);
        list.add(new ZippedIndexedInputFile<>(new File("settest.1"), new IntCompressor()));
        list.add(new ZippedIndexedInputFile<>(new File("settest.2"), new IntCompressor()));

        IndexedInputFileSet<Integer> inset = new IndexedInputFileSet(list);

        inset.lines(0).forEach(s -> System.out.println(s));

        System.out.println(Arrays.toString(inset.data(0)));

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