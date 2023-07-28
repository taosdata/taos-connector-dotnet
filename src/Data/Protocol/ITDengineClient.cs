using System;
using TDengine.Data.Client;

namespace TDengine.Data.Protocol
{
    public interface ITDengineClient
    {
        void Open(TDengineConnectionStringBuilder connectionStringBuilder, ref object connect);
        string GetServerVersion(object connection);
        void ChangeDatabase(object connection, string db);
        ITDengineRows Statement(object connection, string sql, Lazy<TDengineParameterCollection> parameters);
        void Close(object connection);
    }
}