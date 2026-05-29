using System;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Driver.Test.Client.Query
{
    public class MockWSServer
    {
        private HttpListener _httpListener;
        private readonly CancellationTokenSource _cts;
        private Task _serverTask;
        private TcpListener _portGuard;

        public int Port { get; private set; }
        private string Url => $"http://127.0.0.1:{Port}/";

        private Action<WebSocket, WebSocketMessageType, byte[]> _onMessage;

        public MockWSServer(int port, Action<WebSocket, WebSocketMessageType, byte[]> onMessage)
        {
            Port = port;
            _onMessage = onMessage;
            _cts = new CancellationTokenSource();
        }

        /// <summary>
        /// Creates a MockWSServer on an OS-assigned free port, eliminating TOCTOU race.
        /// The port is held by a TcpListener guard until Start() is called.
        /// </summary>
        public static MockWSServer CreateOnFreePort(Action<WebSocket, WebSocketMessageType, byte[]> onMessage)
        {
            var guard = new TcpListener(IPAddress.Loopback, 0);
            guard.Start();
            var port = ((IPEndPoint)guard.LocalEndpoint).Port;

            var server = new MockWSServer(port, onMessage);
            server._portGuard = guard;
            return server;
        }

        public void Start()
        {
            StartWithRetry(3);
        }

        private void StartWithRetry(int maxAttempts)
        {
            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                // Release the port guard immediately before HttpListener binds.
                if (_portGuard != null)
                {
                    _portGuard.Stop();
                    _portGuard = null;
                }

                _httpListener = new HttpListener();
                TryAddPrefix($"http://127.0.0.1:{Port}/");
                TryAddPrefix($"http://localhost:{Port}/");

                try
                {
                    _httpListener.Start();
                }
                catch (Exception)
                {
                    // Bind failed - port was stolen between guard release and Start()
                    if (attempt == maxAttempts - 1) throw;
                    ReallocatePort();
                    continue;
                }

                // Verify the socket is actually listening by probing TCP connect.
                // This catches edge cases where Start() appears to succeed but the
                // port isn't reachable (e.g. SO_REUSEADDR conflicts on Linux).
                if (VerifyListening())
                {
                    break;
                }

                // Not reachable - clean up and retry
                try { _httpListener.Stop(); } catch { }
                try { _httpListener.Close(); } catch { }
                if (attempt == maxAttempts - 1)
                    throw new InvalidOperationException(
                        $"MockWSServer: port {Port} not reachable after {maxAttempts} attempts");
                ReallocatePort();
            }

            _serverTask = Task.Factory.StartNew(() => RunServer(_cts.Token), _cts.Token, TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }

        private bool VerifyListening()
        {
            try
            {
                using (var probe = new TcpClient())
                {
                    probe.Connect(IPAddress.Loopback, Port);
                }
                return true;
            }
            catch (SocketException)
            {
                return false;
            }
        }

        private void ReallocatePort()
        {
            var guard = new TcpListener(IPAddress.Loopback, 0);
            guard.Start();
            Port = ((IPEndPoint)guard.LocalEndpoint).Port;
            _portGuard = guard;
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

        private async Task RunServer(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var context = await _httpListener.GetContextAsync();
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
                    // Listener stopped or client disconnected prematurely
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

            if (_portGuard != null)
            {
                try { _portGuard.Stop(); }
                catch (SocketException) { }
                _portGuard = null;
            }

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
