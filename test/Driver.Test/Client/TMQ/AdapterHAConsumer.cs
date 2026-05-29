using System;
using System.Collections.Generic;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using Driver.Test.Client.Query;
using Newtonsoft.Json;
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using TDengine.TMQ;
using Xunit;

namespace Driver.Test.Client.TMQ
{
    public class AdapterHAConsumer
    {
        [Fact]
        public void TMQSubscribeShouldSendListInstancesWhenAdapterHAEnabled()
        {
            int port = 0;
            bool? receivedListInstances = null;

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        var subReq = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(raw);
                        receivedListInstances = subReq?.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{port}" }
                        });
                        break;
                }
            });
            port = server.Port;

            try
            {
                server.Start();
                var cfg = BuildTmqConfig(port, true);
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe("test_topic");
                    Assert.NotNull(receivedListInstances);
                    Assert.True(receivedListInstances.Value);
                }
                finally
                {
                    consumer.Close();
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void TMQSubscribeShouldNotSendListInstancesWhenAdapterHADisabled()
        {
            bool? receivedListInstances = null;
            bool subscribeReceived = false;

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        subscribeReceived = true;
                        var subReq = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(raw);
                        receivedListInstances = subReq?.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var cfg = BuildTmqConfig(port, false);
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe("test_topic");
                    Assert.True(subscribeReceived);
                    Assert.Null(receivedListInstances);
                }
                finally
                {
                    consumer.Close();
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void TMQSubscribeShouldExpandAddressesFromListInstances()
        {
            int firstPort = 0;
            int secondPort = 0;

            AdapterClusterRegistry.Clear();

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{firstPort}", $"127.0.0.1:{secondPort}" }
                        });
                        break;
                }
            });
            firstPort = server.Port;
            secondPort = firstPort + 1;

            try
            {
                server.Start();
                var cfg = BuildTmqConfig(firstPort, true);
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe("test_topic");

                    // Verify cluster was registered globally
                    var seeds = new List<FailoverAddress>
                    {
                        new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}")
                    };
                    var expanded = AdapterClusterRegistry.ExpandIfKnown(seeds);
                    Assert.True(expanded.Count >= 2,
                        $"Expected at least 2 addresses in cluster, got {expanded.Count}");
                }
                finally
                {
                    consumer.Close();
                }
            }
            finally
            {
                server.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        [Fact]
        public void TMQSubscribeWithOldAdapterShouldStillWork()
        {
            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        // Old adapter: no list_instances in response
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            ListInstances = null
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var cfg = BuildTmqConfig(port, true);
                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe("test_topic");
                    // Should not throw - backward compatible
                }
                finally
                {
                    consumer.Close();
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void TMQConsumerShouldExpandFromRegistryAtConstruction()
        {
            AdapterClusterRegistry.Clear();

            var firstConnected = 0;
            var secondConnected = 0;

            var firstServer = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        Interlocked.Increment(ref firstConnected);
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                }
            });

            var secondServer = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        Interlocked.Increment(ref secondConnected);
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                }
            });

            var firstPort = firstServer.Port;
            var secondPort = secondServer.Port;

            // Pre-register a cluster so the consumer expands at construction time
            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}")
            };
            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}"),
                new FailoverAddress("127.0.0.1", secondPort, $"ws://127.0.0.1:{secondPort}")
            };
            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{firstPort}");
            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{secondPort}");

            try
            {
                firstServer.Start();
                secondServer.Start();

                // Only provide firstPort as seed, but registry has both
                var cfg = BuildTmqConfig(firstPort, true);

                // Open two consumers - with least-connections they should distribute
                var consumer1 = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                var consumer2 = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    // Both servers should receive connections due to registry expansion
                    Assert.True(Volatile.Read(ref firstConnected) >= 1,
                        "First server should have at least 1 connection");
                    Assert.True(Volatile.Read(ref secondConnected) >= 1,
                        "Second server should have at least 1 connection (expanded from registry)");
                }
                finally
                {
                    consumer1.Close();
                    consumer2.Close();
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        [Fact]
        public void TMQReconnectShouldUseDiscoveredAddresses()
        {
            int firstPort = 0;
            int secondPort = 0;

            var firstUnavailable = 0;
            var secondConnected = 0;

            AdapterClusterRegistry.Clear();

            // First server: responds normally initially, returns list_instances with second port,
            // then goes unavailable when poll is called
            var firstServer = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                if (Volatile.Read(ref firstUnavailable) == 1)
                {
                    webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "unavailable",
                        CancellationToken.None).GetAwaiter().GetResult();
                    return;
                }

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{firstPort}", $"127.0.0.1:{secondPort}" }
                        });
                        break;
                    case WSTMQAction.TMQPoll:
                        // Mark as unavailable and close to trigger reconnect
                        Interlocked.Exchange(ref firstUnavailable, 1);
                        webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "going down",
                            CancellationToken.None).GetAwaiter().GetResult();
                        break;
                }
            });

            // Second server: normal behavior
            var secondServer = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        Interlocked.Increment(ref secondConnected);
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                    case WSTMQAction.TMQPoll:
                        SendResponse(webSocket, messageType, new WSTMQPollResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            HaveMessage = false,
                            MessageId = 0
                        });
                        break;
                }
            });

            firstPort = firstServer.Port;
            secondPort = secondServer.Port;

            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{firstPort}");
            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{secondPort}");

            try
            {
                firstServer.Start();
                secondServer.Start();

                // Only seed firstPort, adapterHA=true to discover second via subscribe
                var cfg = BuildTmqConfig(firstPort, true);
                cfg["ws.autoReconnect"] = "true";
                cfg["ws.reconnect.retry.count"] = "5";
                cfg["ws.reconnect.interval.ms"] = "100";

                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    consumer.Subscribe("test_topic");

                    // This poll triggers reconnect: first server closes, consumer reconnects to second
                    var result = consumer.Consume(1000);

                    // After reconnect, second server should have been connected
                    Assert.True(Volatile.Read(ref secondConnected) >= 1,
                        "Should have reconnected to second (discovered) server");
                }
                finally
                {
                    consumer.Close();
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        [Fact]
        public void TMQDoSubscribeRetryShouldMergeDiscoveredAddresses()
        {
            int firstPort = 0;
            int secondPort = 0;

            var subscribeCallCount = 0;

            AdapterClusterRegistry.Clear();

            // Server that fails on first subscribe, then succeeds on reconnect subscribe
            var firstServer = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        var count = Interlocked.Increment(ref subscribeCallCount);
                        if (count == 1)
                        {
                            // First subscribe fails to trigger reconnect path
                            webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "subscribe failed",
                                CancellationToken.None).GetAwaiter().GetResult();
                        }
                        else
                        {
                            // Retry subscribe succeeds with list_instances
                            SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                            {
                                Code = 0, Action = baseReq.Action,
                                ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                                ListInstances = new[] { $"127.0.0.1:{firstPort}", $"127.0.0.1:{secondPort}" }
                            });
                        }
                        break;
                }
            });
            firstPort = firstServer.Port;
            secondPort = firstPort + 1;

            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{firstPort}");
            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{secondPort}");

            try
            {
                firstServer.Start();

                var cfg = BuildTmqConfig(firstPort, true);
                cfg["ws.autoReconnect"] = "true";
                cfg["ws.reconnect.retry.count"] = "5";
                cfg["ws.reconnect.interval.ms"] = "100";

                var consumer = new ConsumerBuilder<Dictionary<string, object>>(cfg).Build();
                try
                {
                    // Subscribe should fail first time, reconnect, then succeed
                    consumer.Subscribe("test_topic");

                    // After retry, cluster should have been expanded
                    var seeds = new List<FailoverAddress>
                    {
                        new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}")
                    };
                    var expanded = AdapterClusterRegistry.ExpandIfKnown(seeds);
                    Assert.True(expanded.Count >= 2,
                        $"Expected at least 2 addresses after retry discovery, got {expanded.Count}");
                }
                finally
                {
                    consumer.Close();
                }
            }
            finally
            {
                firstServer.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        #region TMQ Subscribe Overload Tests

        [Fact]
        public void TMQSubscribeNoListInstancesOverloadShouldNotSendFlag()
        {
            bool? receivedListInstances = null;
            bool subscribeReceived = false;

            var server = MockWSServer.CreateOnFreePort( (webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        subscribeReceived = true;
                        var subReq = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(raw);
                        receivedListInstances = subReq?.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var options = new TMQOptions(new Dictionary<string, string>
                {
                    { "td.connect.type", "WebSocket" },
                    { "td.connect.ip", $"127.0.0.1:{port}" },
                    { "td.connect.user", "root" },
                    { "td.connect.pass", "taosdata" },
                    { "group.id", "test_overload" },
                    { "useSSL", "false" }
                });
                var addr = new FailoverAddress("127.0.0.1", port, $"ws://127.0.0.1:{port}");
                var conn = new TMQConnection(options, addr, TimeSpan.FromSeconds(5));

                // Call the 2-param overload: Subscribe(topics, options) which sets listInstances=false
                conn.Subscribe(new List<string> { "test_topic" }, options);

                Assert.True(subscribeReceived);
                Assert.Null(receivedListInstances);
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void TMQSubscribeWithReqIdOverloadShouldNotSendFlag()
        {
            bool? receivedListInstances = null;
            ulong? receivedReqId = null;
            bool subscribeReceived = false;

            var server = MockWSServer.CreateOnFreePort( (webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        subscribeReceived = true;
                        var subReq = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(raw);
                        receivedListInstances = subReq?.Args?.ListInstances;
                        receivedReqId = subReq?.Args?.ReqId;
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var options = new TMQOptions(new Dictionary<string, string>
                {
                    { "td.connect.type", "WebSocket" },
                    { "td.connect.ip", $"127.0.0.1:{port}" },
                    { "td.connect.user", "root" },
                    { "td.connect.pass", "taosdata" },
                    { "group.id", "test_overload_reqid" },
                    { "useSSL", "false" }
                });
                var addr = new FailoverAddress("127.0.0.1", port, $"ws://127.0.0.1:{port}");
                var conn = new TMQConnection(options, addr, TimeSpan.FromSeconds(5));

                // Call the 3-param overload: Subscribe(reqId, topics, options) which sets listInstances=false
                ulong customReqId = 12345678;
                conn.Subscribe(customReqId, new List<string> { "test_topic" }, options);

                Assert.True(subscribeReceived);
                Assert.Null(receivedListInstances);
                Assert.Equal(customReqId, receivedReqId);
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void TMQSubscribeWithListInstancesTrueShouldSendFlag()
        {
            int port = 0;
            bool? receivedListInstances = null;
            bool subscribeReceived = false;

            var server = MockWSServer.CreateOnFreePort( (webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var baseReq = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (baseReq == null) return;

                switch (baseReq.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSTMQAction.TMQSubscribe:
                        subscribeReceived = true;
                        var subReq = JsonConvert.DeserializeObject<WSActionReq<WSTMQSubscribeReq>>(raw);
                        receivedListInstances = subReq?.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSTMQSubscribeResp
                        {
                            Code = 0, Action = baseReq.Action,
                            ReqId = baseReq.Args == null ? 0 : baseReq.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{port}", "192.168.1.1:6041" }
                        });
                        break;
                }
            });
            port = server.Port;

            try
            {
                server.Start();
                var options = new TMQOptions(new Dictionary<string, string>
                {
                    { "td.connect.type", "WebSocket" },
                    { "td.connect.ip", $"127.0.0.1:{port}" },
                    { "td.connect.user", "root" },
                    { "td.connect.pass", "taosdata" },
                    { "group.id", "test_overload_true" },
                    { "useSSL", "false" }
                });
                var addr = new FailoverAddress("127.0.0.1", port, $"ws://127.0.0.1:{port}");
                var conn = new TMQConnection(options, addr, TimeSpan.FromSeconds(5));

                // Call the 3-param overload: Subscribe(topics, options, listInstances=true)
                var resp = conn.Subscribe(new List<string> { "test_topic" }, options, true);

                Assert.True(subscribeReceived);
                Assert.NotNull(receivedListInstances);
                Assert.True(receivedListInstances.Value);
                Assert.NotNull(resp.ListInstances);
                Assert.Equal(2, resp.ListInstances.Length);
            }
            finally
            {
                server.Dispose();
            }
        }

        #endregion

        #region Helper Methods

        private class TestBaseReq
        {
            [JsonProperty("req_id")] public ulong ReqId { get; set; }
        }

        private static Dictionary<string, string> BuildTmqConfig(int port, bool adapterHA)
        {
            var cfg = new Dictionary<string, string>
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", $"test_adapter_ha_{Guid.NewGuid():N}" },
                { "auto.offset.reset", "earliest" },
                { "td.connect.ip", $"127.0.0.1:{port}" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "client.id", $"test_ha_client_{Guid.NewGuid():N}" },
                { "enable.auto.commit", "false" },
                { "msg.with.table.name", "true" },
                { "useSSL", "false" }
            };

            if (adapterHA)
            {
                cfg["ws.adapterHA"] = "true";
            }

            return cfg;
        }

        private static void SendResponse(WebSocket webSocket, WebSocketMessageType messageType, object response)
        {
            var respStr = JsonConvert.SerializeObject(response);
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
            webSocket.SendAsync(data, messageType, true, CancellationToken.None).GetAwaiter().GetResult();
        }

private static void ResetFailoverCacheConnectionCount(string cacheKey)
        {
            var cacheType = typeof(FailoverAddress).Assembly.GetType("TDengine.Driver.FailoverAddressCache");
            if (cacheType == null) return;

            var syncLockField = cacheType.GetField("SyncLock",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            var countsField = cacheType.GetField("ConnectionCountByAddress",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            if (syncLockField == null || countsField == null) return;

            var syncLock = syncLockField.GetValue(null);
            var counts = countsField.GetValue(null) as Dictionary<string, int>;
            if (syncLock == null || counts == null) return;

            lock (syncLock)
            {
                counts[cacheKey] = 0;
            }
        }

        #endregion
    }
}
