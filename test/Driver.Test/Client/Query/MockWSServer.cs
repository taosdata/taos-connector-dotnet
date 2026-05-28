using System;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Driver.Test.Client.Query
{
    public class MockWSServer
    {
        private readonly HttpListener _httpListener;
        private readonly CancellationTokenSource _cts;
        private readonly TaskCompletionSource<bool> _ready;
        private Task _serverTask;

        private readonly int _port;
        private string Url => $"http://127.0.0.1:{_port}/";

        private Action<WebSocket, WebSocketMessageType, byte[]> _onMessage;

        public MockWSServer(int port, Action<WebSocket, WebSocketMessageType, byte[]> onMessage)
        {
            _port = port;
            _onMessage = onMessage;
            _httpListener = new HttpListener();
            TryAddPrefix(Url);
            TryAddPrefix($"http://localhost:{_port}/");
            _cts = new CancellationTokenSource();
            _ready = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private void TryAddPrefix(string prefix)
        {
            try
            {
                _httpListener.Prefixes.Add(prefix);
            }
            catch (HttpListenerException)
            {
            }
            catch (PlatformNotSupportedException)
            {
            }
        }

        public void Start()
        {
            _httpListener.Start();
            _serverTask = Task.Factory.StartNew(() => RunServer(_cts.Token), _cts.Token, TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
            if (!_ready.Task.Wait(TimeSpan.FromSeconds(2)))
            {
                throw new TimeoutException("mock websocket server failed to start listening in time");
            }

            // Verify the listener is truly accepting connections (needed on Linux CI
            // where HttpListener may not be ready even after GetContextAsync is issued)
            WaitUntilPortAcceptsConnections();
        }

        private void WaitUntilPortAcceptsConnections()
        {
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    using (var tcp = new System.Net.Sockets.TcpClient())
                    {
                        tcp.Connect(IPAddress.Loopback, _port);
                        return;
                    }
                }
                catch (System.Net.Sockets.SocketException)
                {
                    Thread.Sleep(10);
                }
            }

            throw new TimeoutException($"mock websocket server port {_port} not accepting connections");
        }

        private async Task RunServer(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var pendingAccept = _httpListener.GetContextAsync();
                    _ready.TrySetResult(true);
                    var context = await pendingAccept;
                    if (context.Request.IsWebSocketRequest)
                    {
                        var webSocketContext = await context.AcceptWebSocketAsync(null);
                        _ = Task.Run(() => HandleWebSocketConnection(webSocketContext.WebSocket, cancellationToken),
                            cancellationToken);
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (HttpListenerException)
                {
                    // Client disconnected before sending a full request (e.g. TCP health probe)
                }
            }
        }

        private async Task HandleWebSocketConnection(WebSocket webSocket, CancellationToken cancellationToken)
        {
            var buffer = new byte[1024];

            try
            {
                while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
                {
                    var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken);

                    switch (result.MessageType)
                    {
                        case WebSocketMessageType.Text:
                        case WebSocketMessageType.Binary:
                        {
                            var partialBuffer = new byte[result.Count];
                            Array.Copy(buffer, 0, partialBuffer, 0, result.Count);
                            _onMessage(webSocket, result.MessageType, partialBuffer);
                            break;
                        }
                        case WebSocketMessageType.Close:
                            await webSocket.CloseAsync(
                                WebSocketCloseStatus.NormalClosure,
                                "Connection closed",
                                cancellationToken);
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                webSocket.Dispose();
            }
        }

        public void Dispose()
        {
            _cts?.Cancel();

            // Give the server task a chance to complete gracefully
            if (_serverTask != null)
            {
                try
                {
                    // Wait a bit for graceful shutdown
                    if (!_serverTask.Wait(TimeSpan.FromSeconds(2)))
                    {
                        // If it doesn't complete in time, we'll stop the listener anyway
                        // This might throw, but we'll catch it
                    }
                }
                catch (AggregateException ae) when (ae.InnerException is OperationCanceledException)
                {
                    // Expected when task is cancelled
                }
            }

            if (_httpListener != null)
            {
                try
                {
                    if (_httpListener.IsListening)
                    {
                        _httpListener.Stop();
                        _httpListener.Close();
                    }
                }
                catch (ObjectDisposedException)
                {
                    // Already disposed, ignore
                }
                catch (HttpListenerException)
                {
                    // Handle other HTTP listener exceptions
                }
            }
        }
    }
}
