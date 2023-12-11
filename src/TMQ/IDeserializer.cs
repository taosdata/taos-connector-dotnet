using System;
using System.Collections.Generic;
using TDengine.Driver;


namespace  TDengine.TMQ
{
    public interface IDeserializer<T>
    {
        T Deserialize(ITMQRows data, bool isNull, SerializationContext context);
    }
}
