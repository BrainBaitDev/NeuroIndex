/*
 * NeuroIndex .NET/C# Client
 * Connects to NeuroIndex RESP protocol server
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace NeuroIndex
{
    public class NeuroIndexClient : IDisposable
    {
        private readonly string _host;
        private readonly int _port;
        private TcpClient _client;
        private NetworkStream _stream;

        public NeuroIndexClient(string host = "localhost", int port = 6381)
        {
            _host = host;
            _port = port;
        }

        public async Task ConnectAsync()
        {
            _client = new TcpClient();
            await _client.ConnectAsync(_host, _port);
            _stream = _client.GetStream();
            Console.WriteLine($"✓ Connected to NeuroIndex at {_host}:{_port}");
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _client?.Dispose();
            Console.WriteLine("✓ Connection closed");
        }

        private async Task SendCommandAsync(params string[] args)
        {
            // Build RESP array
            var sb = new StringBuilder();
            sb.Append($"*{args.Length}\r\n");

            foreach (var arg in args)
            {
                var bytes = Encoding.UTF8.GetByteCount(arg);
                sb.Append($"${bytes}\r\n{arg}\r\n");
            }

            var data = Encoding.UTF8.GetBytes(sb.ToString());
            await _stream.WriteAsync(data, 0, data.Length);
        }

        private async Task<object> ReadResponseAsync()
        {
            var line = await ReadLineAsync();
            if (string.IsNullOrEmpty(line))
                return null;

            var respType = line[0];
            var data = line.Substring(1);

            switch (respType)
            {
                case '+': // Simple string
                    return data;

                case '-': // Error
                    throw new Exception($"Server error: {data}");

                case ':': // Integer
                    return long.Parse(data);

                case '$': // Bulk string
                    {
                        var length = int.Parse(data);
                        if (length == -1)
                            return null;

                        var buffer = new byte[length];
                        await _stream.ReadAsync(buffer, 0, length);
                        await ReadLineAsync(); // Read trailing \r\n

                        return Encoding.UTF8.GetString(buffer);
                    }

                case '*': // Array
                    {
                        var count = int.Parse(data);
                        if (count == -1)
                            return null;

                        var array = new List<object>();
                        for (int i = 0; i < count; i++)
                        {
                            array.Add(await ReadResponseAsync());
                        }
                        return array;
                    }

                default:
                    throw new Exception($"Unknown RESP type: {respType}");
            }
        }

        private async Task<string> ReadLineAsync()
        {
            var sb = new StringBuilder();
            while (true)
            {
                var b = _stream.ReadByte();
                if (b == -1)
                    return null;

                if (b == '\r')
                {
                    _stream.ReadByte(); // Read \n
                    break;
                }

                sb.Append((char)b);
            }
            return sb.ToString();
        }

        // Command methods

        public async Task<string> PingAsync(string message = null)
        {
            if (message != null)
                await SendCommandAsync("PING", message);
            else
                await SendCommandAsync("PING");

            return (string)await ReadResponseAsync();
        }

        public async Task<string> EchoAsync(string message)
        {
            await SendCommandAsync("ECHO", message);
            return (string)await ReadResponseAsync();
        }

        public async Task<string> SetAsync(string key, string value)
        {
            await SendCommandAsync("SET", key, value);
            return (string)await ReadResponseAsync();
        }

        public async Task<string> GetAsync(string key)
        {
            await SendCommandAsync("GET", key);
            return (string)await ReadResponseAsync();
        }

        public async Task<long> DelAsync(params string[] keys)
        {
            var args = new List<string> { "DEL" };
            args.AddRange(keys);
            await SendCommandAsync(args.ToArray());
            return (long)await ReadResponseAsync();
        }

        public async Task<long> ExistsAsync(params string[] keys)
        {
            var args = new List<string> { "EXISTS" };
            args.AddRange(keys);
            await SendCommandAsync(args.ToArray());
            return (long)await ReadResponseAsync();
        }

        public async Task<List<object>> MGetAsync(params string[] keys)
        {
            var args = new List<string> { "MGET" };
            args.AddRange(keys);
            await SendCommandAsync(args.ToArray());
            return (List<object>)await ReadResponseAsync();
        }

        public async Task<string> MSetAsync(params string[] pairs)
        {
            if (pairs.Length % 2 != 0)
                throw new ArgumentException("MSET requires an even number of arguments");

            var args = new List<string> { "MSET" };
            args.AddRange(pairs);
            await SendCommandAsync(args.ToArray());
            return (string)await ReadResponseAsync();
        }

        public async Task<List<object>> KeysAsync(string pattern = "*")
        {
            await SendCommandAsync("KEYS", pattern);
            return (List<object>)await ReadResponseAsync();
        }

        public async Task<long> DbSizeAsync()
        {
            await SendCommandAsync("DBSIZE");
            return (long)await ReadResponseAsync();
        }

        public async Task<string> InfoAsync()
        {
            await SendCommandAsync("INFO");
            return (string)await ReadResponseAsync();
        }
    }

    // Example usage
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("NeuroIndex .NET Client Demo");
            Console.WriteLine(new string('=', 50));

            using (var client = new NeuroIndexClient("localhost", 6381))
            {
                try
                {
                    await client.ConnectAsync();

                    // Test ping
                    Console.WriteLine($"\nPING: {await client.PingAsync()}");
                    Console.WriteLine($"PING with message: {await client.PingAsync("Hello NeuroIndex!")}");

                    // Set and get
                    Console.WriteLine($"\nSET user:1 'Alice': {await client.SetAsync("user:1", "Alice")}");
                    Console.WriteLine($"GET user:1: {await client.GetAsync("user:1")}");

                    // Multiple operations
                    Console.WriteLine($"\nMSET: {await client.MSetAsync("user:2", "Bob", "user:3", "Charlie", "user:4", "David")}");
                    var mget = await client.MGetAsync("user:1", "user:2", "user:3", "user:4");
                    Console.WriteLine($"MGET: [{string.Join(", ", mget)}]");

                    // Exists and delete
                    Console.WriteLine($"\nEXISTS user:1 user:5: {await client.ExistsAsync("user:1", "user:5")}");
                    Console.WriteLine($"DEL user:1: {await client.DelAsync("user:1")}");
                    Console.WriteLine($"EXISTS user:1: {await client.ExistsAsync("user:1")}");

                    // Database info
                    Console.WriteLine($"\nDBSIZE: {await client.DbSizeAsync()}");
                    var keys = await client.KeysAsync();
                    Console.WriteLine($"KEYS: [{string.Join(", ", keys)}]");

                    // Server info
                    Console.WriteLine($"\nINFO:\n{await client.InfoAsync()}");

                    Console.WriteLine("\n" + new string('=', 50));
                    Console.WriteLine("✓ All operations completed successfully!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\n✗ Error: {ex.Message}");
                }
            }
        }
    }
}
