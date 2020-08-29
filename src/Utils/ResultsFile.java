package Utils;

import DataTypes.DataPair;
import DataTypes.ResultsDataType;
import Kmers.KmerDiff;
import Kmers.KmerWithData;
import Zip.ZipOrNot;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

public class ResultsFile<S,M> implements AutoCloseable
{
    public ResultsFile(File f, ResultsDataType<S,M> resultsDataType) throws IOException
    {
        in = ZipOrNot.getBufferedReader(f);
        this.resultsDataType = resultsDataType;
    }

    public Stream<KmerWithData<DataPair<S, Set<DataPair<KmerDiff,M>>>>> stream()
    {
        return in.lines().map(l -> resultsDataType.fromString(l));
    }

    public void close() throws IOException
    {
        in.close();
    }

    private BufferedReader in;
    private ResultsDataType<S,M> resultsDataType;
}
