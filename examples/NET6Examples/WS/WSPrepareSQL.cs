using System.Text;
#nullable enable
namespace Examples.WS
{
    internal class WSPrepareData
    {
        public string DB { get; set; }
        public string Table { get; set; }
        public bool IfStable { get; set; }

        public WSPrepareData(string db, string table, bool ifStable)
        {
            DB = db;
            Table = table;
            IfStable = ifStable;
        }
        public string CreateDB()
        {
            string sql;
            sql = $"create database if not exists {DB} keep 3650";
            return sql;
        }
        public string CreateTable()
        {
            string sql;
            if (IfStable)
            {
                sql = $"create table if not exists {DB}.{Table} " +
                $"(ts timestamp,bl bool" +
                $",i8 tinyint,i16 smallint,i32 int,i64 bigint" +
                $",u8 tinyint unsigned,u16 smallint unsigned,u32 int unsigned,u64 bigint unsigned" +
                $",f32 float,d64 double" +
                $",bnr binary(50),nchr nchar(50))" +
                $"tags(t_ts timestamp,t_bl bool" +
                $",t_i8 tinyint,t_i16 smallint,t_i32 int,t_i64 bigint" +
                $",t_u8 tinyint unsigned,t_u16 smallint unsigned,t_u32 int unsigned,t_u64 bigint unsigned," +
                $"t_f32 float,t_d64 double" +
                $",t_bnr binary(50),t_nchr nchar(50));";
            }
            else
            {
                sql = $"create table if not exists {DB}.{Table} " +
                $"(ts timestamp,bl bool" +
                $",i8 tinyint,i16 smallint,i32 int,i64 bigint" +
                $",u8 tinyint unsigned,u16 smallint unsigned,u32 int unsigned,u64 bigint unsigned" +
                $",f32 float,d64 double" +
                $",bnr binary(50),nchr nchar(50))";
            }
            return sql;
        }

        public string CreateJTable()
        {
            string sql = $"create table if not exists {DB}.{Table} " +
                $"(ts timestamp,bl bool" +
                $",i8 tinyint,i16 smallint,i32 int,i64 bigint" +
                $",u8 tinyint unsigned,u16 smallint unsigned,u32 int unsigned,u64 bigint unsigned" +
                $",f32 float,d64 double" +
                $",bnr binary(50),nchr nchar(50))" +
                $"tags(json_tag json);";
            return sql;
        }

        public string InsertTable(int numOfRow, string? childTable, long begin = 1656677700000)
        {
            long ts = begin;
            string cTable;
            if (IfStable == true)
            {

                if (string.IsNullOrEmpty(childTable))
                    cTable = $"{Table}_01";
                else
                    cTable = childTable;
            }
            else
            {
                cTable = string.Empty;
            }

            return BuildInsert(numOfRow, cTable, ref ts);
        }

        public string InsertJTable(int numOfRows, string? childTable, long begin = 1656677700000)
        {

            StringBuilder sb = new StringBuilder();
            long ts = begin;
            string cTable = $"{Table}_j01";
            if (!string.IsNullOrEmpty(childTable))
            {
                cTable = childTable;
            }
            ts += 1;
            sb.Append("insert into ");
            sb.Append(childTable);
            sb.Append(" using ");
            sb.Append(Table);
            sb.Append(" tags ");
            sb.Append('(');
            sb.Append("\'{\"jtag_timestamp\":");
            sb.Append(ts);
            sb.Append(",\"jtag_bool\":false,\"jtag_num\":3.141592653,\"jtag_str\":\"beijing\",\"jtag_null\":null}\'");
            sb.Append(')');
            sb.Append(" values");
            for (int i = 0; i < numOfRows; i++)
            {
                sb.Append('(');
                sb.Append(ts);
                ts = ts + 100;
                sb.Append(',');
                sb.Append(i % 2 == 0 ? "true" : "false");
                sb.Append(',');
                sb.Append(i + 0); //tiny int
                sb.Append(',');
                sb.Append(i + 1); // short
                sb.Append(',');
                sb.Append(i + 2); //int
                sb.Append(',');
                sb.Append(i + 3); //long
                sb.Append(',');
                sb.Append(i + 0); //byte
                sb.Append(',');
                sb.Append(i + 1); //ushort
                sb.Append(',');
                sb.Append(i + 2); //uint
                sb.Append(',');
                sb.Append(i + 3); //ulong
                sb.Append(',');
                sb.Append(i * 1F); //float
                sb.Append(',');
                sb.Append(i * 2D); //double
                sb.Append(',');
                sb.Append("\'varchar_col_" + i + "\'");
                sb.Append(',');
                sb.Append("\'nchar_col_" + i + "\'");
                sb.Append(')');
            }
            sb.Append(';');
            return sb.ToString();
        }

        private string BuildInsert(int numOfRows, string? childTable, ref long begin)
        {
            StringBuilder sb = new StringBuilder();
            long ts = begin;
            sb.Append("insert into ");

            if (string.IsNullOrEmpty(childTable))
            {
                sb.Append(Table);
            }
            else
            {
                ts += 1;
                sb.Append(childTable);
                sb.Append(" using ");
                sb.Append(Table);
                sb.Append(" tags ");
                sb.Append('(');
                sb.Append(ts);
                sb.Append(',');
                sb.Append(Table.Length % 2 == 0 ? "true" : "false");
                sb.Append(',');
                sb.Append(-1);
                sb.Append(',');
                sb.Append(-2);
                sb.Append(',');
                sb.Append(-3);
                sb.Append(',');
                sb.Append(-4);
                sb.Append(',');
                sb.Append(1);
                sb.Append(',');
                sb.Append(2);
                sb.Append(',');
                sb.Append(3);
                sb.Append(',');
                sb.Append(4);
                sb.Append(',');
                sb.Append(5.0F);
                sb.Append(',');
                sb.Append(5.550D);
                sb.Append(',');
                sb.Append("\'varchar_tag\'");
                sb.Append(',');
                sb.Append("\'nchar_tag\'");
                sb.Append(')');
            }
            sb.Append(" values");
            for (int i = 0; i < numOfRows; i++)
            {
                sb.Append('(');
                sb.Append(ts);
                ts = ts + 100;
                sb.Append(',');
                sb.Append(i % 2 == 0 ? "true" : "false");
                sb.Append(',');
                sb.Append(i + 0); //tiny int
                sb.Append(',');
                sb.Append(i + 1); // short
                sb.Append(',');
                sb.Append(i + 2); //int
                sb.Append(',');
                sb.Append(i + 3); //long
                sb.Append(',');
                sb.Append(i + 0); //byte
                sb.Append(',');
                sb.Append(i + 1); //ushort
                sb.Append(',');
                sb.Append(i + 2); //uint
                sb.Append(',');
                sb.Append(i + 3); //ulong
                sb.Append(',');
                sb.Append(i * 1F); //float
                sb.Append(',');
                sb.Append(i * 2D); //double
                sb.Append(',');
                sb.Append("\'varchar_col_" + i + "\'");
                sb.Append(',');
                sb.Append("\'nchar_col_" + i + "\'");
                sb.Append(')');
            }
            sb.Append(';');
            return sb.ToString();
        }

        public string SelectTable()
        {
            return $"select * from {DB}.{Table}";
        }

        public string DropTable()
        {
            return $"drop table if exists {DB}.{Table}";
        }
    }
}
