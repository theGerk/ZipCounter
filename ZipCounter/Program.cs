using CsvHelper;
using System.Linq;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Diagnostics.CodeAnalysis;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System;

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


namespace ZipCounter
{
	class Program
	{
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
		}

		static async Task<Task> runRequest(int i)
		{
			var asyncRequest = http.GetStreamAsync(string.Format(Resource1.ReadUriFormatString, i));
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

			using var writerStream = new StreamWriter($"../../../Output/{identifier}.csv");
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
