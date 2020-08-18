package DataTypes;

public class DataPair<A,B>
{
    public DataPair(A a, B b)
    {
        this.a = a;
        this.b = b;
    }

    public A getA()
    {
        return a;
    }

    public B getB()
    {
        return b;
    }

    private A a;
    private B b;
}
