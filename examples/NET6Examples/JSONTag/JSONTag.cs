using Examples.UtilsTools;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace Examples.JSONTag
{
    internal class JSONTagExample
    {
        public void RunJSONTag(IntPtr conn)
        {
            Tools.ExecuteUpdate(conn,"create database if not exists json_db keep 3650");
            Tools.ExecuteUpdate(conn,"use json_db");
            Tools.ExecuteUpdate(conn, "create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json);");
            Tools.ExecuteUpdate(conn, "insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 1, false, 'json1', '涛思数据') (1591060608000, 23, true, '涛思数据', 'json')");
            Tools.ExecuteUpdate(conn, "insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')");
            Tools.ExecuteUpdate(conn, "insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')");
            Tools.ExecuteUpdate(conn, "insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')");
            Tools.ExecuteUpdate(conn, "insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '涛思数据', 'ewe')");
            Tools.ExecuteUpdate(conn, "insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '涛思数据','')");
            Tools.ExecuteUpdate(conn, "insert into jsons1_7 using jsons1 tags('{\"tag1\":\"涛思数据\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '涛思数据', 'dws')");

            IntPtr res = Tools.ExecuteErrorQuery(conn, "select * from jsons1");
            Display(res);
            NativeMethods.FreeResult(res);

            res = Tools.ExecuteErrorQuery(conn, "select jtag->\"tag1\",jtag->\"tag2\",jtag->\"tag3\" from jsons1; ");
            Display(res);
            NativeMethods.FreeResult(res);

            res = Tools.ExecuteErrorQuery(conn, "select ts,dataint,databool,datastr,datastrbin,jtag->\"tag1\",jtag->\"tag2\",jtag->\"tag3\" from jsons1 where ts>1591060608000 and jtag->\"tag3\" is null;");
            Display(res);
            NativeMethods.FreeResult(res);

            Tools.ExecuteUpdate(conn, "drop database if exists json_db");

        }

        public void Display(IntPtr res)
        {
            List<TDengineMeta> metaList = NativeMethods.FetchFields(res);
            List<object> dataList = NativeMethods.GetData(res);

            metaList.ForEach(meta =>
            {
                Console.Write("{0} {1}({2})",meta.name,meta.TypeName(),meta.size);
            });
            Console.WriteLine("");

            for (int i = 0; i < dataList.Count; i++)
            {
                if (i > 0 && (i + 1) % metaList.Count == 0)
                {
                    Console.WriteLine("{0}\t|", dataList[i]);
                }
                else
                {
                    Console.Write("{0}\t|", dataList[i]);
                }
            }

        }
    }
}
