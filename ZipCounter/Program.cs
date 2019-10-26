using CsvHelper;
using System.Linq;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;

public struct InputRow
{
	public int ZipCode { get; set; }
	public int ZipPlus4 { get; set; }
	//public int CustomerID { get; set; }
	//public string FirstName { get; set; }
	//public string LastName { get; set; }
	//public int PhoneNumber { get; set; }
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
			Task<Task>[] readTasks = new Task<Task>[ReadFiles];
			Task[] writeTasks = new Task[ReadFiles];
			//do work
			for (int i = 0; i < ReadFiles; i++)
				readTasks[i] = runRequest(i + 1);

			//wait for readin work to be done and get the write tasks
			for (int i = 0; i < ReadFiles; i++)
				writeTasks[i] = readTasks[i].Result;

			//get cummulative report
			var cummulative = new List<InputRow>();
			foreach (var item in Total)
				cummulative.AddRange(item);

			//do cummulative report and wait for it to be done
			MakeReport(cummulative, "cummulative").Wait();

			//wait for all the other writes to be done
			for (int i = 0; i < ReadFiles; i++)
				writeTasks[i].Wait();
		}

		static async Task<Task> runRequest(int i)
		{
			var asyncRequest = http.GetStreamAsync(string.Format(getUrlFormatString, i));
			using StreamReader stream = new StreamReader(await asyncRequest);
			using CsvReader reader = new CsvReader(stream);
			reader.Configuration.HasHeaderRecord = true;
			var inputRows = reader.GetRecords<InputRow>().ToArray();
			Total.Add(inputRows);
			return MakeReport(inputRows, $"report{i:00}");
		}


		static async Task MakeReport(IList<InputRow> data, string identifier)
		{
			//Generate dictionary
			Dictionary<int, List<InputRow>> zipCount = new Dictionary<int, List<InputRow>>();
			foreach (var row in data)
			{
				if (!zipCount.ContainsKey(row.ZipCode))
					zipCount[row.ZipCode] = new List<InputRow>();
				zipCount[row.ZipCode].Add(row);
			}

			List<OutputRow> output = new List<OutputRow>();

			foreach (var item in zipCount)
			{
				var uniqueCount = 0;
				HashSet<InputRow> usedAddresses = new HashSet<InputRow>();
				foreach (var addr in item.Value)
				{
					if (usedAddresses.Contains(addr))
						continue;

					usedAddresses.Add(addr);
					uniqueCount++;
				}


				output.Add(new OutputRow()
				{
					ZipCode = item.Key,
					NumberOfOccurences = item.Value.Count,
					NumberOfUniqueAddresses = uniqueCount
				});
			}

			using var writerStream = new StreamWriter($"../../../{identifier}.csv");
			using var csvWriter = new CsvWriter(writerStream);
			csvWriter.WriteRecords(output.OrderBy(x => x.ZipCode));
		}
	}


	public struct OutputRow
	{
		public int ZipCode { get; set; }
		public int NumberOfOccurences { get; set; }
		public int NumberOfUniqueAddresses { get; set; }
	}

}
