namespace TDengine.TMQ
{
    public interface ITMQRows
    {
        object GetValue(int ordinal);
        bool Read();
        int FieldCount { get; }
        string TableName { get; }
        string GetName(int ordinal);
    }
}