using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.IO;
using ContractsPrototype.PlanParser;

namespace ContractsPrototype.ContractsDemo
{
    /// <summary>
    /// Interaction logic for Window1.xaml
    /// </summary>
    public partial class Window1 : Window
    {
        public Window1()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {

        }

        private void button1_Click(object sender, RoutedEventArgs e)
        {
            Microsoft.Win32.OpenFileDialog dlg = new Microsoft.Win32.OpenFileDialog();
            dlg.FileName = "Select query plan";
            dlg.DefaultExt = ".xml";
            dlg.Filter = "XML documents (.xml) |*.xml";

            Nullable<bool> result = dlg.ShowDialog();

            if (result == true)
            {
                this.textBox1.Text = dlg.FileName;
            }
        }

        private void button3_Click(object sender, RoutedEventArgs e)
        {
            String message = "Query plan analyzer (search for consistent accordance)\n" +
                             "(c) 2010 Rafael J. Fernández-Moctezuma (rfernand@cs.pdx.edu)\n\n" +
                             "This program is provided AS-IS, and is a reference implementation of the consistent-accordance finding work reported in my doctoral dissertation.\n\n" +
                             "Permission to use, modify, or adapt is given for non-commercial use, provided credit is given to me.";

            MessageBox.Show(message, "About", MessageBoxButton.OK, MessageBoxImage.Information, MessageBoxResult.OK, MessageBoxOptions.DefaultDesktopOnly);
        }

        private void button2_Click(object sender, RoutedEventArgs e)
        {
            Plan p = new Plan();
            p = Parser.ParseXML(textBox1.Text);
            Plan p2 = p.ConsistentAccordance();

            //if plan has a consistent accordance,
            if (p2.HasOfferings())
            {
                MessageBox.Show("The plan has consistent accordance(s):\n" + p2.ToString());
            }
            else
            {
                MessageBox.Show("The plan has no consistent accordances.");
            }
        }
    }
}
