using System;
using System.Collections.Generic;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class FailoverConnectorTests
    {
        [Fact]
        public void TryOpenShouldContinueToNextAddressWhenCurrentLeaseFails()
        {
            var first = new FailoverAddress("first", 6030, Guid.NewGuid().ToString("N"));
            var second = new FailoverAddress("second", 6031, Guid.NewGuid().ToString("N"));
            var attempts = new List<string>();
            FailoverAddressLease lease = null;

            try
            {
                var opened = FailoverConnector.TryOpen(
                    new[] { first, second },
                    1,
                    0,
                    false,
                    null,
                    address =>
                    {
                        attempts.Add(address.CacheKey);
                        if (ReferenceEquals(address, first))
                        {
                            throw new InvalidOperationException("first address failed");
                        }

                        return address.Host;
                    },
                    out var connection,
                    out lease,
                    out var lastException);

                Assert.True(opened);
                Assert.Equal("second", connection);
                Assert.Same(second, lease.Address);
                Assert.IsType<InvalidOperationException>(lastException);
                Assert.Equal(new[] { first.CacheKey, second.CacheKey }, attempts);
            }
            finally
            {
                lease?.Dispose();
            }
        }

        [Fact]
        public void TryOpenShouldSkipFailedPreferredAddressWithinSameAttempt()
        {
            var first = new FailoverAddress("first", 6030, Guid.NewGuid().ToString("N"));
            var second = new FailoverAddress("second", 6031, Guid.NewGuid().ToString("N"));
            var attempts = new List<string>();
            FailoverAddressLease lease = null;

            try
            {
                var opened = FailoverConnector.TryOpen(
                    new[] { first, second },
                    1,
                    0,
                    false,
                    first,
                    address =>
                    {
                        attempts.Add(address.CacheKey);
                        if (ReferenceEquals(address, first))
                        {
                            throw new InvalidOperationException("preferred address failed");
                        }

                        return address.Host;
                    },
                    out var connection,
                    out lease,
                    out var lastException);

                Assert.True(opened);
                Assert.Equal("second", connection);
                Assert.Same(second, lease.Address);
                Assert.IsType<InvalidOperationException>(lastException);
                Assert.Equal(new[] { first.CacheKey, second.CacheKey }, attempts);
            }
            finally
            {
                lease?.Dispose();
            }
        }
    }
}
