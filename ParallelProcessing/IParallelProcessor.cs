using System;

namespace ParallelProcessing
{
    public interface IParallelProcessor<TInput, TOutput> : IDisposable
    {
        void ProcessObject(TInput input);

        IObservable<TOutput> GetObservable();
    }
}