using NUnit.Framework;
using TDengine.Data.Client;

namespace TDengine.Data.Tests
{
    [TestFixture]
    public class TDengineParameterCollectionTests
    {
        [Test]
        public void Add_AddsParameterToCollection()
        {
            var collection = new TDengineParameterCollection();
            var parameter = new TDengineParameter("@param1", 10);

            var index = collection.Add(parameter);

            Assert.AreEqual(0, index);
            Assert.AreEqual(1, collection.Count);
            Assert.AreEqual(parameter, collection[0]);
        }

        [Test]
        public void Clear_RemovesAllParametersFromCollection()
        {
            var collection = new TDengineParameterCollection();
            collection.Add(new TDengineParameter("@param1", 10));
                collection.Add(new TDengineParameter("@2", "test"));
                
            collection.Clear();
            
            Assert.AreEqual(0, collection.Count);
        }

        [Test]
        public void Contains_ReturnsTrueIfParameterExistsInCollection()
        {
            var collection = new TDengineParameterCollection();
            var parameter = new TDengineParameter("@param1", 10);
            collection.Add(parameter);
            
            var containsParameter = collection.Contains(parameter);
            
            Assert.IsTrue(containsParameter);
        }

        [Test]
        public void Contains_ReturnsFalseIfParameterDoesNotExistInCollection()
        {
            var collection = new TDengineParameterCollection();
            var parameter = new TDengineParameter("@param1", 10);
            
            var containsParameter = collection.Contains(parameter);
            
            Assert.IsFalse(containsParameter);
        }
    }
}