using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Transactions;
using System.Windows.Forms;
using Dapper;
using SqlBulkCopySample.Extensions;
using SqlBulkCopySample.Model;

namespace SqlBulkCopySample
{
    public partial class Form1 : Form
    {
        private static readonly string ConnectionString =
            File.ReadAllText(
                Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "connectionstring.txt"));

        public Form1()
        {
            this.InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            var objCount = 300000;
            var objs =
                Enumerable.Range(0, objCount).Select(i => new TableTable { Id = i, Name = "MyName " + i }).ToArray();

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    var sb = new StringBuilder();
                    sb.AppendLine(@"INSERT INTO [dbo].[TableTable]");
                    sb.AppendLine(@"       ([Id],");
                    sb.AppendLine(@"        [Name])");
                    sb.AppendLine(@"VALUES");
                    sb.AppendLine(@"       (@Id,");
                    sb.AppendLine(@"        @Name);");

                    sql.Execute(sb.ToString(), objs);
                }

                tx.Complete();
            }

            stopwatch.Stop();

            MessageBox.Show(stopwatch.ElapsedMilliseconds.ToString());
        }

        private void button2_Click(object sender, EventArgs e)
        {
            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Name", typeof(string));

            for (var i = 0; i < 300000; i++)
            {
                var row = dt.NewRow();
                row["Id"] = i;
                row["Name"] = "MyName " + i;

                dt.Rows.Add(row);
            }

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    sql.Open();

                    using (var sqlBulkCopy = new SqlBulkCopy(sql))
                    {
                        sqlBulkCopy.DestinationTableName = "dbo.TableTable";
                        sqlBulkCopy.WriteToServer(dt);
                    }
                }

                tx.Complete();
            }

            stopwatch.Stop();

            MessageBox.Show(stopwatch.ElapsedMilliseconds.ToString());
        }
    }
}