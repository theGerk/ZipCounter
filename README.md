# How to run the code.
1. Clone the repo.
1. Navigate into the repo's root folder.
2. Change directory into ZipCounter.
3. Run command `dotnet run`.

After the program is run the output will be in the Output folder which is located in the repo's root.

---

### Output Explained
There will be a bunch of .csv files. All of these files correlate one of the inital .csv files given, with the exception of Cummulative.csv which is generated based on a concactenation of all the .csv files given.

Each .csv file contains three columns (with headers).

|Column Name|Description|
|-|-|
|ZipCode|A zip code.|
|NumberOfOccurences|The number of times the corresponding zip code was found.|
|NumberOfUniqueAddresses|The number of different addresses found having the corresponding zip code.|

The .csv files are sorted by zip code from least to greatest.
