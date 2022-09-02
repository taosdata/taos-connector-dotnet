using TDengineDriver;
using TDengineWS.Impl;

namespace Examples.WS
{

    internal class WebSocketSample
    {
        string createDB;
        string dropDB;
        string createTable;
        string insert;
        string select;
        string Table;
        string DB;

        public WebSocketSample(string db, string table)
        {
            createDB = $"create database if not exists {db} keep 3650";
            dropDB = $"drop database if exists {db}";
            createTable = $"create table if not exists {db}.{table} " +
                $"(ts timestamp,bl bool" +
                $",i8 tinyint,i16 smallint,i32 int,i64 bigint" +
                $",u8 tinyint unsigned,u16 smallint unsigned,u32 int unsigned,u64 bigint unsigned" +
                $",f32 float,d64 double" +
                $",bnr binary(50),nchr nchar(50))" +
                $"tags(t_bl bool" +
                $",t_i8 tinyint,t_i16 smallint,t_i32 int,t_i64 bigint" +
                $",t_u8 tinyint unsigned,t_u16 smallint unsigned,t_u32 int unsigned,t_u64 bigint unsigned," +
                $"t_f32 float,t_d64 double" +
                $",t_bnr binary(50),t_nchr nchar(50));";

            insert = $"insert into {db}.{table}_01 using {db}.{table} " +
                $"tags(true,-1,-2,-3,-4,1,2,3,4,3.1415,3.14159265358979,'bnr_tag_1','ncr_tag_1')" +
                $"values" +
                $"(1658286671000,true,-1,-2,-3,-4,1,2,3,4,3.1415,3.141592654,'binary_col_1','nchar_col_1')" +
                $"(1658286672000,false,-2,-3,-4,-5,2,3,4,5,6.283,6.283185308,'binary_col_2','nchar_col_2')" +
                $"(1658286673000,true,-3,-4,-5,-6,3,4,5,6,9.4245,9.424777962,'binary_col_3','nchar_col_3')" +
                $"(1658286674000,false,-4,-5,-6,-7,4,5,6,7,12.566,12.566370616,'binary_col_4','nchar_col_4')" +
                $"(1658286675000,true,-5,-6,-7,-8,5,6,7,8,15.707500000000001,15.70796327,'binary_col_5','nchar_col_5');" +
                $"(1658286676000,null,null,null,null,null,null,null,null,null,null,null,null,null);";
            Console.WriteLine(createDB);
            Console.WriteLine(createTable);
            Console.WriteLine(insert);
            select = $"select * from {db}.{table}";
            DB = db;
            Table = table;
        }

        public void RunWS(string dsn)
        {

            IntPtr wsConn = LibTaosWS.WSConnectWithDSN(dsn);
            ValidWsConn(wsConn);

            IntPtr wsRes = LibTaosWS.WSQuery(wsConn, createDB);
            ValidQuery(wsRes);
            Console.WriteLine("create database {0} success, cost {1} nanoseconds", DB, LibTaosWS.WSTakeTiming(wsRes));
            Display(wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            wsRes = LibTaosWS.WSQuery(wsConn, createTable);
            ValidQuery(wsRes);
            Console.WriteLine("create table {0} success, cost {1} nanoseconds", Table, LibTaosWS.WSTakeTiming(wsRes));
            Display(wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            wsRes = LibTaosWS.WSQuery(wsConn, insert);
            ValidQuery(wsRes);
            Console.WriteLine("insert table {0}_01 success, cost {1} nanoseconds", Table, LibTaosWS.WSTakeTiming(wsRes));
            Display(wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            wsRes = LibTaosWS.WSQueryTimeout(wsConn, select, 1);
            ValidQuery(wsRes);
            Console.WriteLine("select table {0} success, cost {1} nanoseconds", Table, LibTaosWS.WSTakeTiming(wsRes));
            Display(wsRes);
            LibTaosWS.WSFreeResult(wsRes);

            wsRes = LibTaosWS.WSQuery(wsConn, dropDB);
            ValidQuery(wsRes);
            Console.WriteLine("drop database {0} success, cost {1} nanoseconds", DB, LibTaosWS.WSTakeTiming(wsRes));
            Display(wsRes);
            LibTaosWS.WSFreeResult(wsRes);


            LibTaosWS.WSClose(wsConn);


        }

        internal void Display(IntPtr wsRes)
        {

            if (LibTaosWS.WSIsUpdateQuery(wsRes) == true)
            {
                Console.WriteLine($"affect {LibTaosWS.WSAffectRows(wsRes)} rows");
            }
            else
            {
                List<TDengineMeta> metas = LibTaosWS.WSGetFields(wsRes);
                foreach (var meta in metas)
                {
                    Console.Write("{0} {1}({2}) \t|\t", meta.name, meta.TypeName(), meta.size);
                }
                Console.WriteLine("");
                List<object> dataSet = LibTaosWS.WSGetData(wsRes);
                for (int i = 0; i < dataSet.Count;)
                {
                    for (int j = 0; j < metas.Count; j++)
                    {
                        Console.Write("{0}\t|\t", dataSet[i]);
                        i++;
                    }
                    Console.WriteLine("");
                }
            }
            Console.WriteLine("");
        }

        internal void ValidWsConn(IntPtr wsConn)
        {
            if (wsConn == IntPtr.Zero)
            {
                throw new Exception($"get WS connection failed,reason:{LibTaosWS.WSErrorStr(IntPtr.Zero)} code:{LibTaosWS.WSErrorNo(IntPtr.Zero)}");
            }
        }
        internal void ValidQuery(IntPtr wsRes)
        {
            int code = LibTaosWS.WSErrorNo(wsRes);
            if (code != 0)
            {
                throw new Exception($"execute SQL failed: reason: {LibTaosWS.WSErrorStr(wsRes)}, code:{code}");
            }
        }
    }
}
