using System;

namespace ParallelProcessing
{
    public interface IParallelProcessor : IDisposable
    {
        void ProcessObject<TInput, TOutput>(TInput input, Func<TInput, TOutput> processor, Action<TInput, TOutput, Exception> callback);
        void Stop();
        bool WaitForCompletion(TimeSpan timeout);
    }
}