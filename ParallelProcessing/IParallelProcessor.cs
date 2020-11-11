using System;

namespace ParallelProcessing
{
    public interface IParallelProcessor<TInput, TOutput> : IDisposable
    {
        void ProcessObject(TInput input, Action<TInput, TOutput, Exception> callback);
        void Stop();
        bool WaitForCompletion(TimeSpan timeout);
    }
}