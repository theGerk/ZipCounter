using System;
using System.Net.Http;

namespace CSharp
{
	class Program
	{
		public static readonly HttpClient http = new HttpClient();

		static void Main(string[] args)
		{
			var formatStr = "blahblah{0}";
			Console.WriteLine(formatStr, 1);
			Console.WriteLine(formatStr, 2);
		}
	}
}
