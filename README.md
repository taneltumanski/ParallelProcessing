# ParallelProcessing

A small library I made for continuous parallel processing.
We had a use case where we needed to process video as separate frames in a row in an ordered fashion but our initial solution only maxed out one core.
We tried using tasks and dedicated threads but they were hard to use and messy so I made this library.

## Usage

```csharp
var observer = Observer.Create<int>(x => Console.WriteLine(x));

var parallelProcessor = new ParallelProcessorBuilder<string, int>()
    .AsOrdered() // Make sure the output is coming in the same order we added the input
    .WithThreadCount(4) // How many threads we want to create
    .WithBlockingAdd() // Do we want to wait on add when a thread is available?
    .WithThreadPriority(ThreadPriority.Highest)
    .WithProcessor(x => int.Parse(x)) // Function that does the converting or a typed class that does the same
    .ObserveWith(observer) // Observe the processor and get the results
    .Build();

using (parallelProcessor)
{
    parallelProcessor.ProcessObject("5");
    parallelProcessor.ProcessObject("10");
    parallelProcessor.ProcessObject("15");
    parallelProcessor.ProcessObject("20");
}
```

Good luck.
