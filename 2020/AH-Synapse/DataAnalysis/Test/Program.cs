using System.IO;
using ChoETL;

string csv = @"Id, Name
1, Tom
2, Mark";
//csv = File.ReadAllText("");

using (var r = ChoCSVReader.LoadText(csv)
    .WithFirstLineHeader()
    .WithMaxScanRows(2)
    .QuoteAllFields()
    )
{
    using (var w = new ChoParquetWriter("*** Your Parquet file ***"))
    {
        w.Write(r);
    }
}