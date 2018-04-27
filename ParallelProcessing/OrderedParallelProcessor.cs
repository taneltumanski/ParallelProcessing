using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class OrderedParallelProcessor<TInput, TOutput> : ParallelProcessor<TInput, TOutput>
    {
        private readonly ConcurrentQueue<long> _idQueue = new ConcurrentQueue<long>();

        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor) : base(processor) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, bool isBlockingAdd) : base(processor, isBlockingAdd) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, ThreadPriority threadPriority) : base(processor, threadPriority) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, int threadCount) : base(processor, threadCount) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, int threadCount, ThreadPriority threadPriority, bool isBlockingAdd) : base(processor, threadCount, threadPriority, isBlockingAdd) { }

        protected override void AddInput(WrappedObject<TInput> wrappedObject)
        {
            _idQueue.Enqueue(wrappedObject.Id);

            base.AddInput(wrappedObject);
        }

        protected override IObservable<WrappedObject<TOutput>> GetInternalObservable()
        {
            return Observable.Create<WrappedObject<TOutput>>(observer =>
            {
                var cachedObjects = new ConcurrentDictionary<long, WrappedObject<TOutput>>();

                var orderedObserver = Observer
                    .Create<WrappedObject<TOutput>>(next =>
                    {
                        var sent = true;
                        long nextId;

                        if (_idQueue.TryPeek(out nextId) && nextId == next.Id)
                        {
                            _idQueue.TryDequeue(out var _);
                            observer.OnNext(next);
                        }
                        else
                        {
                            cachedObjects.TryAdd(next.Id, next);
                            sent = false;
                        }

                        while (sent)
                        {
                            if (_idQueue.TryPeek(out nextId) && cachedObjects.TryRemove(nextId, out var result))
                            {
                                _idQueue.TryDequeue(out var _);
                                observer.OnNext(result);
                            }
                            else
                            {
                                sent = false;
                            }
                        }
                    }, ex => observer.OnError(ex), () => observer.OnCompleted());

                return base
                    .GetInternalObservable()
                    .Subscribe(orderedObserver);
            });
        }
    }
}
