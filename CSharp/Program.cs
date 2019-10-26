using System;
using System.Net.Http;
using CsvHelper;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft;
using System.Threading.Tasks;

namespace CSharp
{
	public class ZipCodeTally
	{
		public ZipCodeTally() { }
		public ZipCodeTally(IEnumerable<Address> addresses)
		{
			Insert(addresses);
		}

		public void Insert(IEnumerable<Address> addresses)
		{
			foreach (Address address in addresses)
				Insert(address);
		}

		public void Insert(Address address, int count = 1)
		{
			if (zipCount.ContainsKey(address.ZipCode))
				zipCount[address.ZipCode].Insert(address);
			else
				zipCount.Add(address.ZipCode, new SpecificCount(address, count));
			Total += count;
		}

		public void Insert(KeyValuePair<Address, int> addressCount)
		{
			Insert(addressCount.Key, addressCount.Value);
		}

		public void Insert(ZipCodeTally other)
		{
			foreach (var item in other.ZipCodePlus4Totals)
			{
				Insert(item);
			}
		}


		public Dictionary<int, SpecificCount> zipCount { get; private set; } = new Dictionary<int, SpecificCount>();

		public int Total { get; private set; } = 0;
		private IEnumerable<KeyValuePair<int, int>> ZipCodeTotals() => zipCount.Select(x => new KeyValuePair<int, int>(x.Key, x.Value.Total));
		private IEnumerable<KeyValuePair<Address, int>> ZipCodePlus4Totals => zipCount.SelectMany(x => x.Value.Counter.Select(y => new KeyValuePair<Address, int>(new Address() { ZipCode = x.Key, ZipPlus4 = y.Key }, y.Value)));
		private IEnumerable<KeyValuePair<int, IEnumerable<KeyValuePair<int, int>>>> ZipCodePlus4ByZipCodes => zipCount.Select(x => new KeyValuePair<int, IEnumerable<KeyValuePair<int, int>>>(x.Key, x.Value.Counter.Select(y => new KeyValuePair<int, int>(y.Key, y.Value))));

		public class SpecificCount
		{
			public Dictionary<string, object> JSObjectLike()
			{
				var output = new Dictionary<string, object>();
				output.Add("Total", Total);
				output.Add("ByZipPlus4", Counter);
				return output;
			}

			public SpecificCount(Address address, int count = 1)
			{
				Insert(address, count);
			}

			public void Insert(Address address, int count = 1)
			{
				if (Counter.ContainsKey(address.ZipPlus4))
					Counter[address.ZipPlus4] += count;
				else
					Counter.Add(address.ZipPlus4, count);
			}

			public Dictionary<int, int> Counter { get; set; } = new Dictionary<int, int>();
			public int Total { get; private set; } = 0;
		}
	}


	public struct Address
	{
		public int ZipCode { get; set; }
		public int ZipPlus4 { get; set; }
	}


	class Program
	{
		public static readonly HttpClient http = new HttpClient();

		/// <summary>
		/// Format string, to be formated with a number between 1 and 10 (inclusive) to retrive a CSV with adresses.
		/// </summary>
		public static readonly string getUrlFormatString = "https://journeyblobstorage.blob.core.windows.net/sabpublic/Group{0:00}.csv";

		public static ZipCodeTally Total = new ZipCodeTally();

		public const int ReadFiles = 10;

		static void Main(string[] args)
		{
			Task[] tasks = new Task[10];
			//do work
			for (int i = 0; i < ReadFiles; i++)
				runRequest(i + 1).Wait();

			////wait for work to be done
			//for (int i = 0; i < ReadFiles; i++)
			//	tasks[i].Wait();

			//write final file
			File.WriteAllText($"total_report.json", MakeReport(Total));
		}

		static async Task runRequest(int i)
		{
			var asyncRequest = http.GetStreamAsync(string.Format(getUrlFormatString, i));
			using StreamReader stream = new StreamReader(await asyncRequest);
			using CsvReader reader = new CsvReader(stream);
			reader.Configuration.HasHeaderRecord = true;
			ZipCodeTally current = new ZipCodeTally(reader.GetRecords<Address>());
			Total.Insert(current);
			File.WriteAllText($"report{i:00}.json", MakeReport(current));
		}

		static string MakeReport(ZipCodeTally tally)
		{
			return Newtonsoft.Json.JsonConvert.SerializeObject(tally, Newtonsoft.Json.Formatting.Indented);
		}
	}
}
