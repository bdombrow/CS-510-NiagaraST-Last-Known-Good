using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using ContractsPrototype;
using ContractsPrototype.PlanParser;

namespace ContractsPrototype.DebugConsole
{
    public class Program
    {
        // tests building and pretty-printing
        static void test1()
        {
            Template t = new Template();
            t.Add("a", ClassType.mark.PLUS);
            t.Add("b", ClassType.mark.MINUS);
            t.Add("c", ClassType.mark.HASH);

            Template t2 = new Template();
            t2.Add("a", ClassType.mark.PLUS);
            t2.Add("b", ClassType.mark.MINUS);
            t2.Add("c", ClassType.mark.MINUS);

            Scheme s = new Scheme();
            s.Add(t);
            s.Add(t2);

            Scheme s2 = new Scheme();

            Offering o = new Offering(1, 1);
            o.addInputScheme(1, s);
            o.addOutputScheme(1, s2);
            o.addFeedbackInputScheme(1, Scheme.SchemeAsAny());
            o.addFeedbackOutputScheme(1, Scheme.SchemeAsAny());

            Operator op = new Operator("SELECT", 1, 1);
            op.AddOffering(o);
            op.AddOffering(o);

            Console.WriteLine(op.ToString());
            Console.ReadLine();

        }

        // tests XML parsing, building, and pretty-printing
        static void test2()
        {
            Plan p = Parser.ParseXML(@"C:\Users\rfernand\Desktop\ContractsPrototype\ContractsDemo\Examples\SELECT.xml");
            p.ToString();
            Console.ReadLine();
        }

        // tests consistent accordance finding
        static void test3()
        {
            Plan p = Parser.ParseXML(@"C:\Users\rfernand\Desktop\ContractsPrototype\ContractsDemo\Examples\SELECT.xml");
            p.ConsistentAccordance();
            p.ToString();
            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            test3();
        }
    }
}
