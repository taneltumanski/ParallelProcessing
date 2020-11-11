using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class UpdatableParallelProcessor<TInput, TOutput> : IParallelProcessor<TInput, TOutput>
    {
        private IParallelProcessor<TInput, TOutput> _processor;
        private readonly object _lock = new object();

        public UpdatableParallelProcessor(IParallelProcessor<TInput, TOutput> processor)
        {
            _processor = processor;
        }

        public void ProcessObject(TInput input, Action<TInput, TOutput, Exception> callback)
        {
            int tryCount = 0;

            // Try again few times just incase some requests did not make it before stopping the last processor
            while (true)
            {
                try
                {
                    _processor.ProcessObject(input, callback);

                    return;
                }
                catch (InvalidOperationException)
                {
                    if (tryCount++ > 5)
                    {
                        throw;
                    }

                    Console.WriteLine("RETRY");
                }
            }
        }

        public void Stop()
        {
            _processor.Stop();
        }

        public bool WaitForCompletion(TimeSpan timeout)
        {
            return _processor.WaitForCompletion(timeout);
        }

        public void UpdateProcessor(IParallelProcessor<TInput, TOutput> newProcessor)
        {
            var oldProcessor = Interlocked.Exchange(ref _processor, newProcessor);

            // Wait until en-route requests have made it to the list
            SpinWait.SpinUntil(() => false, 250);

            // Stop adding to the list
            oldProcessor.Stop();
            oldProcessor.WaitForCompletion(TimeSpan.FromSeconds(5));
            oldProcessor.Dispose();
        }

        public void Dispose()
        {
            _processor.Dispose();
        }

        public override bool Equals(object obj)
        {
            return _processor.Equals(obj);
        }

        public override int GetHashCode()
        {
            return _processor.GetHashCode();
        }

        public override string ToString()
        {
            return _processor.ToString();
        }
    }
}
