using CsvHelper;
using System.Linq;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System;
using System.Text;

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

		/// <summary>
		/// Keeps track of all the outputs
		/// </summary>
		public static ConcurrentBag<InputRow[]> Total = new ConcurrentBag<InputRow[]>();


		public static DirectoryInfo OutputFolder = GetOutputDir();
		private static DirectoryInfo GetOutputDir()
		{
			var dir = new DirectoryInfo(Environment.CurrentDirectory);
			for(; dir.Parent != null; dir = dir.Parent)
			{
				var output = dir.EnumerateDirectories().Where(innerDir => innerDir.Name == Resource1.OutputFolderName);
				if (output.Any())
					return output.First();
			}
			return Directory.CreateDirectory(Resource1.OutputFolderName);
		}


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
			MakeReport(cummulative, "Cummulative").Wait();

			//wait for all the other writes to be done
			for (int i = 0; i < ReadFiles; i++)
				writeTasks[i].Wait();
		}

		/// <summary>
		/// Runs the ith request, getting and reading the CSV, then hands off the rest of the work to do computation on the data and writing to a file.
		/// </summary>
		/// <param name="i">which request to run, must be integer in range [1,10]</param>
		/// <returns>An async task (awaiting this will ensure the read is done) the task then contains another task. Awaiting the inner task will ensure the write is done</returns>
		static async Task<Task> runRequest(int i)
		{
			//get Uri by format string
			var inputUri = string.Format(Resource1.ReadUriFormatString, i);

			//make the async reqest to the Uri
			var asyncRequest = http.GetStreamAsync(inputUri);

			//use some disposable resources
			using StreamReader stream = new StreamReader(await asyncRequest);
			using CsvReader reader = new CsvReader(stream);

			//The CSV file has a header record
			reader.Configuration.HasHeaderRecord = true;

			//parse entire CSV file
			var inputRows = reader.GetRecords<InputRow>().ToArray();

			//input the parsed data the cummulative bag
			Total.Add(inputRows);

			//an ID string, will be the name of the output file when this is digested
			string idString = $"Group{i:00}";

			//Just a fun messege
			Console.WriteLine($"Read in {idString} from {inputUri}");

			//generate report and return the async task
			return MakeReport(inputRows, idString);
		}

		/// <summary>
		/// Generates a report based on a collection of input data
		/// <para>The report has three columns: ZipCode, NumberOfOccurences, and NumberOfUniqueOccurences. Occurences is the number of times the corresponding zip code appeared. NumberOfUniqueOccurences is the number of unique addresses that appeared with the corresponding zip code. For example if there are two rows with 221 B Baker Street at zip code 42, then that is a 2 in NumberofOccurences and a 1 in NumberOfUniqueOccurences.</para> 
		/// </summary>
		/// <param name="data">Input data</param>
		/// <param name="identifier">the file to write out to (will have .csv extension appended)</param>
		/// <returns>Asynchonus task to be awaited</returns>
		static async Task MakeReport(IEnumerable<InputRow> data, string identifier)
		{
			//Generate dictionary from zip codes to the collection of all rows with that zip
			Dictionary<string, List<InputRow>> zipCount = new Dictionary<string, List<InputRow>>();
			foreach (var row in data)
			{
				if (!zipCount.ContainsKey(row.ZipCode))
					zipCount[row.ZipCode] = new List<InputRow>();
				zipCount[row.ZipCode].Add(row);
			}

			//List of output data to be generated
			List<OutputRow> output = new List<OutputRow>();

			foreach (var item in zipCount)
			{
				//Find number of unique address in this zip code
				var uniqueCount = 0;
				HashSet<InputRow> usedAddresses = new HashSet<InputRow>();

				//for each address in this zip code
				foreach (var addr in item.Value)
				{
					//check if we've already seen it
					if (usedAddresses.Contains(addr))
						continue;

					//if we haven't seen this address
					usedAddresses.Add(addr);
					uniqueCount++;
				}

				//add the zip code in as a row in the output
				output.Add(new OutputRow()
				{
					ZipCode = item.Key,
					NumberOfOccurences = item.Value.Count,
					NumberOfUniqueAddresses = uniqueCount
				});
			}
			
			//Get the output file's path
			var outputFile = Path.Combine(OutputFolder.FullName, $"{identifier}.csv");

			//create memory strem and write all data into the memory stream
			using var memoryStream = new MemoryStream();
			using var writerStream = new StreamWriter(memoryStream);
			using var csvWriter = new CsvWriter(writerStream);
			csvWriter.WriteRecords(output.OrderBy(x => x.ZipCode));

			//put the memory stream back at position 0 and copy it to the output file asynchronously
			memoryStream.Position = 0;
			await memoryStream.CopyToAsync(new FileStream(outputFile, FileMode.Create));

			//Say hello!
			Console.WriteLine($"Wrote out {identifier} to {outputFile}");
		}
	}


	public struct InputRow
	{
		public string ZipCode { get; set; }
		//public int ZipPlus4 { get; set; }
		//public int CustomerID { get; set; }
		//public string FirstName { get; set; }
		//public string LastName { get; set; }
		//public int PhoneNumber { get; set; }
		public string Address01 { get; set; }
		public string Address02 { get; set; }
		public string City { get; set; }
		public string State { get; set; }
	}



	public struct OutputRow
	{
		public string ZipCode { get; set; }
		public int NumberOfOccurences { get; set; }
		public int NumberOfUniqueAddresses { get; set; }
	}

}
