using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;

namespace ParallelProcessing
{
    public static class ParallelProcessorExtensions
    {
        public static ParallelProcessorBuilder<TIn, TOut> ObserveWith<TIn, TOut>(this ParallelProcessorBuilder<TIn, TOut> builder, Action<ProcessResult<TOut>> action)
        {
            return builder.ObserveWith(Observer.Create(action));
        }

        public static UpdatableParallelProcessor<TIn, TOut> AsUpdatable<TIn, TOut>(this IParallelProcessor<TIn, TOut> processor)
        {
            return new UpdatableParallelProcessor<TIn, TOut>(processor);
        }

        public static Task<TOut> Process<TIn, TOut>(this IParallelProcessor<TIn, TOut> processor, TIn input)
        {
            var tcs = new TaskCompletionSource<TOut>(TaskCreationOptions.RunContinuationsAsynchronously);

            processor.ProcessObject(input, (_, o, ex) =>
            {
                if (ex != null)
                {
                    tcs.TrySetException(ex);
                }
                else
                {
                    tcs.TrySetResult(o);
                }
            });

            return tcs.Task;
        }

        public static IObservable<ProcessResult<TOut>> AsObservable<TIn, TOut>(this IParallelProcessor<TIn, TOut> processor)
        {
            return AsObservable(processor, false);
        }

        public static IObservable<ProcessResult<TOut>> AsObservable<TIn, TOut>(this IParallelProcessor<TIn, TOut> processor, bool disposeProcessor)
        {
            return Observable
                .Using(() => new ObservableProcessor<TIn, TOut>(processor, Enumerable.Empty<IObserver<ProcessResult<TOut>>>(), disposeProcessor), x => x.GetObservable())
                .Publish()
                .RefCount()
                .AsObservable();
        }
    }

    public class ObservableProcessor<TIn, TOut> : IParallelProcessor<TIn, TOut>
    {
        private readonly IParallelProcessor<TIn, TOut> _processor;
        private readonly IDisposable _subscriptions;
        private readonly bool _disposeProcessor;
        private readonly Subject<ProcessResult<TOut>> _subject = new Subject<ProcessResult<TOut>>();

        public ObservableProcessor(IParallelProcessor<TIn, TOut> processor, IEnumerable<IObserver<ProcessResult<TOut>>> observers, bool disposeProcessor)
        {
            _processor = processor;
            _disposeProcessor = disposeProcessor;
            _subscriptions = new CompositeDisposable(observers.Select(x => _subject.Subscribe(x)).ToArray());
        }

        public void ProcessObject(TIn input, Action<TIn, TOut, Exception> callback)
        {
            var actualCallback = new Action<TIn, TOut, Exception>((i, o, ex) =>
            {
                callback?.Invoke(i, o, ex);

                var result = ex == null ? new ProcessResult<TOut>(o) : new ProcessResult<TOut>(ex);

                _subject.OnNext(result);
            });

            _processor.ProcessObject(input, actualCallback);
        }

        public IObservable<ProcessResult<TOut>> GetObservable()
        {
            return _subject.AsObservable();
        }

        public void Stop()
        {
            _processor.Stop();
        }

        public bool WaitForCompletion(TimeSpan timeout)
        {
            return _processor.WaitForCompletion(timeout);
        }

        public void Dispose()
        {
            _subscriptions.Dispose();
            _subject.Dispose();
            
            if (_disposeProcessor)
            {
                _processor.Dispose();
            }
        }
    }

    public readonly struct ProcessResult<T>
    {
        private readonly T _result;

        public Exception Exception { get; }

        public ProcessResult(T result)
        {
            _result = result;
            Exception = null;
        }

        public ProcessResult(Exception ex)
        {
            _result = default;
            Exception = ex;
        }

        public bool TryGetResult(out T result)
        {
            result = _result;

            return Exception == null;
        }
    }
}
