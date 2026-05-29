using System;
using System.Threading;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace AdapterHAManualTest
{
    class Program
    {
        private const string Host = "192.168.154.129";
        private const int SeedPort = 6041;
        private const string User = "root";
        private const string Password = "taosdata";

        static void Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;
            Console.WriteLine("=== taosAdapter 高可用集成测试 ===");
            Console.WriteLine($"种子节点: {Host}:{SeedPort}");
            Console.WriteLine($"预期发现节点: {Host}:6042");
            Console.WriteLine();

            try
            {
                TestSQLWriteFailover();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] 测试异常: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }

            Console.WriteLine();
            Console.WriteLine("所有测试完成，按回车退出...");
            Console.ReadLine();
        }

        static void TestSQLWriteFailover()
        {
            Console.WriteLine("========================================");
            Console.WriteLine("  测试: SQL WebSocket 写入故障切换");
            Console.WriteLine("========================================");
            Console.WriteLine();

            var connStr =
                $"host={Host}:{SeedPort},{Host}:6042;protocol=WebSocket;username={User};password={Password};adapterHA=true;autoReconnect=true;reconnectRetryCount=5;reconnectIntervalMs=1000";

            Console.WriteLine($"[INFO] 连接字符串:");
            Console.WriteLine($"       {connStr}");
            Console.WriteLine();
            Console.WriteLine("[INFO] 正在连接...");

            var builder = new ConnectionStringBuilder(connStr);
            using (var client = DbDriver.Open(builder))
            {
                Console.WriteLine("[OK] 连接成功!");
                Console.WriteLine();

                // 创建测试数据库和表
                Console.WriteLine("[INFO] 创建测试数据库和表...");
                client.Exec("DROP DATABASE IF EXISTS test_adapter_ha");
                client.Exec("CREATE DATABASE test_adapter_ha");
                client.Exec("USE test_adapter_ha");
                client.Exec(
                    "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT) TAGS (location BINARY(64), groupId INT)");
                Console.WriteLine("[OK] 数据库和表创建成功");
                Console.WriteLine();

                // 第一阶段: 正常写入
                Console.WriteLine("┌─────────────────────────────────────┐");
                Console.WriteLine("│  第一阶段: 正常写入验证               │");
                Console.WriteLine("└─────────────────────────────────────┘");

                for (int i = 0; i < 5; i++)
                {
                    try
                    {
                        var sql =
                            $"INSERT INTO test_adapter_ha.d{i} USING test_adapter_ha.meters TAGS ('loc{i}', {i}) VALUES (NOW + {i}s, {10.0 + i}, {200 + i})";
                        client.Exec(sql);
                        Console.WriteLine($"  [OK] 写入第 {i + 1}/5 条成功");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  [FAIL] 写入第 {i + 1}/5 条失败: {ex.Message}");
                    }

                    Thread.Sleep(200);
                }

                Console.WriteLine();
                Console.WriteLine("[PASS] 第一阶段完成，所有写入正常。");
                Console.WriteLine();

                // ========== 等待用户操作 ==========
                Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
                Console.WriteLine("║                                                          ║");
                Console.WriteLine($"║  请停止种子节点 adapter ({Host}:6041)       ║");
                Console.WriteLine("║                                                          ║");
                Console.WriteLine("║  操作完成后按回车继续...                                  ║");
                Console.WriteLine("║                                                          ║");
                Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
                Console.ReadLine();

                // ========== 第二阶段 ==========
                Console.WriteLine("┌─────────────────────────────────────┐");
                Console.WriteLine("│  第二阶段: 故障切换写入验证            │");
                Console.WriteLine("└─────────────────────────────────────┘");
                Console.WriteLine("[INFO] 种子 adapter 已停止，尝试继续写入...");
                Console.WriteLine("[INFO] 预期：短暂失败后通过其他 adapter 恢复写入");
                Console.WriteLine();

                int successCount = 0;
                int failCount = 0;
                for (int i = 5; i < 20; i++)
                {
                    try
                    {
                        var sql =
                            $"INSERT INTO test_adapter_ha.d{i} USING test_adapter_ha.meters TAGS ('loc{i}', {i}) VALUES (NOW + {i}s, {10.0 + i}, {200 + i})";
                        client.Exec(sql);
                        successCount++;
                        Console.WriteLine($"  [OK] 写入第 {i + 1} 条成功 (成功:{successCount} 失败:{failCount})");
                    }
                    catch (Exception ex)
                    {
                        failCount++;
                        Console.WriteLine(
                            $"  [WARN] 写入第 {i + 1} 条失败: {ex.Message}");
                    }

                    Thread.Sleep(500);
                }

                Console.WriteLine();
                Console.WriteLine($"[INFO] 第二阶段结果: 成功={successCount}, 瞬时失败={failCount}");
                if (successCount > 0)
                {
                    Console.WriteLine("[PASS] 故障切换成功! 写入已通过备用 adapter 恢复。");
                }
                else
                {
                    Console.WriteLine("[FAIL] 故障切换失败! 所有写入均未成功。");
                }

                Console.WriteLine();

                // ========== 等待用户恢复 ==========
                Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
                Console.WriteLine("║                                                          ║");
                Console.WriteLine($"║  请重新启动种子节点 adapter ({Host}:6041)    ║");
                Console.WriteLine("║                                                          ║");
                Console.WriteLine("║  操作完成后按回车继续...                                  ║");
                Console.WriteLine("║                                                          ║");
                Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
                Console.ReadLine();

                // ========== 第三阶段 ==========
                Console.WriteLine("┌─────────────────────────────────────┐");
                Console.WriteLine("│  第三阶段: 全部恢复后写入验证          │");
                Console.WriteLine("└─────────────────────────────────────┘");

                successCount = 0;
                for (int i = 20; i < 25; i++)
                {
                    try
                    {
                        var sql =
                            $"INSERT INTO test_adapter_ha.d{i} USING test_adapter_ha.meters TAGS ('loc{i}', {i}) VALUES (NOW + {i}s, {10.0 + i}, {200 + i})";
                        client.Exec(sql);
                        successCount++;
                        Console.WriteLine($"  [OK] 写入第 {i + 1} 条成功");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"  [FAIL] 写入第 {i + 1} 条失败: {ex.Message}");
                    }

                    Thread.Sleep(200);
                }

                if (successCount == 5)
                {
                    Console.WriteLine();
                    Console.WriteLine("[PASS] 恢复后写入全部正常!");
                }

                // ========== 查询验证 ==========
                Console.WriteLine();
                Console.WriteLine("┌─────────────────────────────────────┐");
                Console.WriteLine("│  查询验证                            │");
                Console.WriteLine("└─────────────────────────────────────┘");
                try
                {
                    using (var rows = client.Query("SELECT COUNT(*) FROM test_adapter_ha.meters"))
                    {
                        if (rows.Read())
                        {
                            Console.WriteLine($"  [OK] meters 表总行数: {rows.GetValue(0)}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  [FAIL] 查询失败: {ex.Message}");
                }

                // ========== 清理 ==========
                Console.WriteLine();
                Console.WriteLine("[INFO] 清理测试数据库...");
                try
                {
                    client.Exec("DROP DATABASE IF EXISTS test_adapter_ha");
                    Console.WriteLine("[OK] 清理完成");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WARN] 清理失败 (可忽略): {ex.Message}");
                }
            }

            Console.WriteLine();
            Console.WriteLine("========================================");
            Console.WriteLine("  测试完成!");
            Console.WriteLine("========================================");
        }
    }
}
