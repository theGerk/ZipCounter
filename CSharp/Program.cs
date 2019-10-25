using System;
using System.Net.Http;
using CsvHelper;
using System.IO;
using System.Linq;
using System.Collections.Generic;

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


		Dictionary<int, SpecificCount> zipCount = new Dictionary<int, SpecificCount>();

		public int Total { get; private set; } = 0;
		public IEnumerable<KeyValuePair<int, int>> ZipCodeTotals {
			get => zipCount.Select(x => new KeyValuePair<int, int>(x.Key, x.Value.Total));
		}
		public IEnumerable<KeyValuePair<Address, int>> ZipCodePlus4Totals {
			get => zipCount.SelectMany(x => x.Value.Counter.Select(y => new KeyValuePair<Address, int>(new Address() { ZipCode = x.Key, ZipPlus4 = y.Key }, y.Value)));
		}
		public IEnumerable<KeyValuePair<int, IEnumerable<KeyValuePair<int, int>>>> ZipCodePlus4ByZipCodes {
			get => zipCount.Select(x => new KeyValuePair<int, IEnumerable<KeyValuePair<int, int>>>(x.Key, x.Value.Counter.Select(y => new KeyValuePair<int, int>(y.Key, y.Value))));
		}

		class SpecificCount
		{
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

		static async void Main(string[] args)
		{

			for (int i = 1; i <= 10; i++)
			{
				var asyncRequest = http.GetStreamAsync(string.Format(getUrlFormatString, i));
				asyncRequest.ContinueWith<Stream>()
				using StreamReader stream = new StreamReader(await );
				using CsvReader reader = new CsvReader(stream);
				reader.Configuration.HasHeaderRecord = true;
				ZipCodeTally current = new ZipCodeTally(reader.GetRecords<Address>());
				Total.Insert(current);
				await http.PostAsync(MakeReport(current);
			}
			MakeReport(Total);
		}

		static string MakeReport(ZipCodeTally tally)
		{

		}
	}
}
