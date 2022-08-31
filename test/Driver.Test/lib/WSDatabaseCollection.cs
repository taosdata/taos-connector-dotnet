using Test.WSFixture;
using Xunit;

[CollectionDefinition("WS Database collection")]
public class WSDatabaseCollection : ICollectionFixture<WSDataBaseFixture>
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}




