using System;
using System.Collections.Generic;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class AdapterHATests
    {
        [Fact]
        public void ConnectionStringBuilderAdapterHADefaultFalse()
        {
            var builder = new ConnectionStringBuilder("host=localhost;port=6041;protocol=WebSocket;username=root;password=taosdata");
            Assert.False(builder.AdapterHA);
        }

        [Fact]
        public void ConnectionStringBuilderAdapterHAParsesTrue()
        {
            var builder = new ConnectionStringBuilder("host=localhost;port=6041;protocol=WebSocket;username=root;password=taosdata;adapterHA=true");
            Assert.True(builder.AdapterHA);
        }

        [Fact]
        public void ConnectionStringBuilderAdapterHAParsesFalse()
        {
            var builder = new ConnectionStringBuilder("host=localhost;port=6041;protocol=WebSocket;username=root;password=taosdata;adapterHA=false");
            Assert.False(builder.AdapterHA);
        }

        [Fact]
        public void ConnectionStringBuilderAdapterHASetterWorks()
        {
            var builder = new ConnectionStringBuilder("host=localhost;port=6041;protocol=WebSocket;username=root;password=taosdata");
            builder.AdapterHA = true;
            Assert.True(builder.AdapterHA);
        }

        [Fact]
        public void AdapterHAHelperMergesNewAddresses()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var discovered = new[] { "localhost:6042", "localhost:6044" };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, discovered, TDengineConstant.ProtocolWebSocket, false);

            Assert.NotNull(newAddresses);
            Assert.Equal(2, newAddresses.Count);
            Assert.Equal("localhost", newAddresses[0].Host);
            Assert.Equal(6042, newAddresses[0].Port);
            Assert.Equal("localhost", newAddresses[1].Host);
            Assert.Equal(6044, newAddresses[1].Port);
        }

        [Fact]
        public void AdapterHAHelperSkipsDuplicateAddresses()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var discovered = new[] { "localhost:6041", "localhost:6042" };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, discovered, TDengineConstant.ProtocolWebSocket, false);

            Assert.NotNull(newAddresses);
            Assert.Single(newAddresses);
            Assert.Equal(6042, newAddresses[0].Port);
        }

        [Fact]
        public void AdapterHAHelperSkipsInvalidAddresses()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var discovered = new[] { "", "   ", null, "localhost:6042" };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, discovered, TDengineConstant.ProtocolWebSocket, false);

            Assert.NotNull(newAddresses);
            Assert.Single(newAddresses);
            Assert.Equal(6042, newAddresses[0].Port);
        }

        [Fact]
        public void AdapterHAHelperReturnsNullWhenNoNewAddresses()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var discovered = new[] { "localhost:6041" };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, discovered, TDengineConstant.ProtocolWebSocket, false);

            Assert.Null(newAddresses);
        }

        [Fact]
        public void AdapterHAHelperReturnsNullWhenDiscoveredIsEmpty()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, Array.Empty<string>(), TDengineConstant.ProtocolWebSocket, false);

            Assert.Null(newAddresses);
        }

        [Fact]
        public void AdapterHAHelperReturnsNullWhenDiscoveredIsNull()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, null, TDengineConstant.ProtocolWebSocket, false);

            Assert.Null(newAddresses);
        }

        [Fact]
        public void AdapterClusterRegistryExpandsKnownCluster()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041"),
                new FailoverAddress("localhost", 6042, "ws://localhost:6042"),
                new FailoverAddress("localhost", 6044, "ws://localhost:6044")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            var expanded = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Equal(3, expanded.Count);
        }

        [Fact]
        public void AdapterClusterRegistryReturnsOriginalWhenNotKnown()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("unknown", 9999, "ws://unknown:9999")
            };

            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Same(seeds, result);
        }

        [Fact]
        public void AdapterHAHelperSkipsAddressWithoutPort()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "ws://localhost:6041")
            };

            // "hostonly" has no port - should be skipped since port will be 0
            var discovered = new[] { "hostonly", "localhost:6042" };

            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, discovered, TDengineConstant.ProtocolWebSocket, false);

            Assert.NotNull(newAddresses);
            Assert.Single(newAddresses);
            Assert.Equal(6042, newAddresses[0].Port);
        }

        #region AdapterClusterRegistry Guard Tests

        [Fact]
        public void RegisterClusterWithNullSeedsDoesNothing()
        {
            AdapterClusterRegistry.Clear();

            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            AdapterClusterRegistry.RegisterCluster(null, fullCluster);

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };
            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Same(seeds, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void RegisterClusterWithEmptySeedsDoesNothing()
        {
            AdapterClusterRegistry.Clear();

            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            AdapterClusterRegistry.RegisterCluster(new List<FailoverAddress>(), fullCluster);

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };
            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Same(seeds, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void RegisterClusterWithNullFullClusterDoesNothing()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, null);

            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Same(seeds, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void RegisterClusterWithEmptyFullClusterDoesNothing()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, new List<FailoverAddress>());

            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Same(seeds, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void RegisterClusterSkipsWhitespaceSeedCacheKey()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "  "),
                new FailoverAddress("host2", 6041, "ws://host2:6041")
            };

            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041"),
                new FailoverAddress("host3", 6041, "ws://host3:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            // Whitespace seed key should be skipped, host2 seed should still register
            var lookup = new List<FailoverAddress>
            {
                new FailoverAddress("host2", 6041, "ws://host2:6041")
            };
            var expanded = AdapterClusterRegistry.ExpandIfKnown(lookup);
            Assert.Equal(3, expanded.Count);

            // Whitespace key should not have been registered
            var whitespaceLookup = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "  ")
            };
            var result = AdapterClusterRegistry.ExpandIfKnown(whitespaceLookup);
            Assert.Same(whitespaceLookup, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void RegisterClusterSkipsWhitespaceFullClusterCacheKey()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "   "),
                new FailoverAddress("host3", 6041, "ws://host3:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            // host3 from fullCluster should be registered as a key
            var host3Lookup = new List<FailoverAddress>
            {
                new FailoverAddress("host3", 6041, "ws://host3:6041")
            };
            var expanded = AdapterClusterRegistry.ExpandIfKnown(host3Lookup);
            Assert.Equal(3, expanded.Count);

            // Whitespace key in fullCluster should not be registered
            var whitespaceLookup = new List<FailoverAddress>
            {
                new FailoverAddress("host2", 6041, "   ")
            };
            var result = AdapterClusterRegistry.ExpandIfKnown(whitespaceLookup);
            Assert.Same(whitespaceLookup, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void ExpandIfKnownWithNullSeedsReturnsNull()
        {
            AdapterClusterRegistry.Clear();

            var result = AdapterClusterRegistry.ExpandIfKnown(null);
            Assert.Null(result);
        }

        [Fact]
        public void ExpandIfKnownWithEmptySeedsReturnsEmpty()
        {
            AdapterClusterRegistry.Clear();

            var empty = new List<FailoverAddress>();
            var result = AdapterClusterRegistry.ExpandIfKnown(empty);
            Assert.Same(empty, result);
        }

        [Fact]
        public void ExpandIfKnownSkipsWhitespaceCacheKey()
        {
            AdapterClusterRegistry.Clear();

            // Register a cluster under host1
            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };
            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041"),
                new FailoverAddress("host3", 6041, "ws://host3:6041")
            };
            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            // Try expand with first seed having whitespace key - should skip it
            // and not find anything since the second key doesn't match
            var lookupWithWhitespace = new List<FailoverAddress>
            {
                new FailoverAddress("x", 1, ""),
                new FailoverAddress("y", 2, "  ")
            };
            var result = AdapterClusterRegistry.ExpandIfKnown(lookupWithWhitespace);
            Assert.Same(lookupWithWhitespace, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void RegisterClusterAlwaysUpdatesToAuthoritativeList()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            var largeCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041"),
                new FailoverAddress("host3", 6041, "ws://host3:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, largeCluster);

            // Register a smaller cluster (e.g. node decommissioned) — should be accepted
            var smallerCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, smallerCluster);

            // Should return the updated (smaller) authoritative cluster
            var expanded = AdapterClusterRegistry.ExpandIfKnown(seeds);
            Assert.Equal(2, expanded.Count);

            AdapterClusterRegistry.Clear();
        }

        #endregion
    }
}
