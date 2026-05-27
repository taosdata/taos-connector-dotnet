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
    }
}
