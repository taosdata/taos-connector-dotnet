namespace Benchmark
{
    public class EntryPoint
    {
        string Host = "127.0.0.1";
        string User = "root";
        string Passwd = "taosdata";
        short Port = 6031;
        string benchmarkOptions = "connect";
        string RunTimes = "1";
        string TableTypes = "normal";
        // private string Rest;
        static void Main(string[] args)
        {

            EntryPoint entryPoint = new EntryPoint();
            entryPoint.ReadArgs(args);
            entryPoint.RunBenchMark(entryPoint.benchmarkOptions, entryPoint.TableTypes, entryPoint.RunTimes);

        }

        public void PrintHelp()
        {

            string indent = "\t\t\t";
            string indent2 = "\t\t";

            Console.WriteLine("\t -s {0}{1}", indent, "Benchmark stage, \"prepare\"，\"connect\",\"insert\",\"query\",\"avg\",\"clean\",default \"connect\"");
            Console.WriteLine("\t -t {0}{1}", indent, "Benchmark data type, table with\"json\" tag,table with \"normal\" column type,default \"normal\"");
            Console.WriteLine("\t -n {0}{1}", indent, "number of times to run");
            Console.WriteLine("\t --help {0}{1}", indent2, "Print help info");
            System.Environment.Exit(0);

        }

        public void ReadArgs(string[] args)
        {
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
                    case "--help":
                        this.PrintHelp();
                        break;
                    case "-s":
                        this.benchmarkOptions = args[i + 1];
                        i++;
                        break;
                    case "-n":
                        this.RunTimes = args[i + 1];
                        i++;
                        break;
                    case "-t":
                        this.TableTypes = args[i + 1];
                        i++;
                        break;
                    default:
                        Console.WriteLine("Unsupported option {0}", args[i]);
                        System.Environment.Exit(0);
                        break;
                }

            }

        }


        public void RunBenchMark(string options,string tableTypes,string times)
        {
            switch (benchmarkOptions)
            {
                case "prepare":
                    Prepare prepare = new Prepare(this.Host,this.User,this.Passwd,this.Port);
                    prepare.Run(TableTypes);
                    break;
                case "insert":
                    Insert insert = new Insert(this.Host, this.User, this.Passwd, this.Port);
                    insert.Run(TableTypes,int.Parse(times));
                    break;
                case "query":
                    Query query = new Query(this.Host, this.User, this.Passwd, this.Port);
                    query.Run(TableTypes, int.Parse(times));
                    break;
                case "avg":
                    Aggregate aggregate = new Aggregate(this.Host, this.User, this.Passwd, this.Port);
                    aggregate.Run(TableTypes,int.Parse(times));
                    break;
                case "clean":
                    CleanUp cleanUp = new CleanUp(this.Host, this.User, this.Passwd, this.Port);
                    cleanUp.Run(TableTypes, int.Parse(times));
                    break;
                default:
                    Connect connect = new Connect(this.Host, this.User, this.Passwd, this.Port);
                    connect.Run(TableTypes);
                    break;
            }
        }
    }
}

