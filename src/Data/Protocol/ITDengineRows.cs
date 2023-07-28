using System;

namespace TDengine.Data.Protocol
{
    public interface ITDengineRows : IDisposable
    {
        bool HasRows { get; }
        int AffectRows { get; }
        int FieldCount { get; }
        new void Dispose();
        long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length);
        char GetChar(int ordinal);
        long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length);
        string GetDataTypeName(int ordinal);
        object GetValue(int ordinal);
        Type GetFieldType(int ordinal);
        int GetFieldSize(int ordinal);
        string GetName(int ordinal);
        int GetOrdinal(string name);
        bool Read();
    }
}