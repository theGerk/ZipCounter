using CsvHelper;
using System.Linq;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using System.Collections.Generic;
using System.Collections.Concurrent;

public struct InputRow
{
	public int ZipCode { get; set; }
	public int ZipPlus4 { get; set; }
	public int CustomerID { get; set; }
	public string FirstName { get; set; }
	public string LastName { get; set; }
	public int PhoneNumber { get; set; }
	public string Address01 { get; set; }
	public string Address02 { get; set; }
	public string City { get; set; }
	public string State { get; set; }
}


public struct AddressCount
{

}


namespace ZipCounter
{
	class Program
	{
		/// <summary>
		/// Format string, to be formated with a number between 1 and ReadFiles (inclusive) to retrive a CSV with adresses.
		/// </summary>
		public static readonly string getUrlFormatString = "https://journeyblobstorage.blob.core.windows.net/sabpublic/Group{0:00}.csv";

		/// <summary>
		/// The number of fiels to be read
		/// </summary>
		public const int ReadFiles = 10;

		/// <summary>
		/// Global HTTP client object
		/// </summary>
		public static readonly HttpClient http = new HttpClient();

		public static ConcurrentBag<InputRow[]> Total = new ConcurrentBag<InputRow[]>();

		static void Main(string[] args)
		{
			Task[] tasks = new Task[ReadFiles];
			//do work
			for (int i = 0; i < ReadFiles; i++)
				runRequest(i + 1);

			//wait for work to be done
			for (int i = 0; i < ReadFiles; i++)
				tasks[i].Wait();

			//write final file
			var total = new List<InputRow>();
			foreach (var item in Total)
				total.AddRange(item);
			MakeReport(total, "cummulative")
		}

		static async Task runRequest(int i)
		{
			var asyncRequest = http.GetStreamAsync(string.Format(getUrlFormatString, i));
			using StreamReader stream = new StreamReader(await asyncRequest);
			using CsvReader reader = new CsvReader(stream);
			reader.Configuration.HasHeaderRecord = true;
			var inputRows = reader.GetRecords<InputRow>().ToArray();
			Total.Add(inputRows);
			MakeReport(inputRows, $"report {i:00}");
		}


		static void MakeReport(IList<InputRow> data, string identifier)
		{
			//Each line is an address

		}
	}
}
