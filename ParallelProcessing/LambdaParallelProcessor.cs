using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class LambdaParallelProcessor<TInput, TOutput> : ParallelProcessor<TInput, TOutput>
    {
        public LambdaParallelProcessor(Func<TInput, TOutput> action) : this(Environment.ProcessorCount, action) { }
        public LambdaParallelProcessor(bool isBlockingAdd, Func<TInput, TOutput> action) : this(Environment.ProcessorCount, ThreadPriority.Normal, isBlockingAdd, action) { }
        public LambdaParallelProcessor(ThreadPriority threadPriority, Func<TInput, TOutput> action) : this(Environment.ProcessorCount, threadPriority, false, action) { }
        public LambdaParallelProcessor(int threadCount, Func<TInput, TOutput> action) : this(threadCount, ThreadPriority.Normal, false, action) { }
        public LambdaParallelProcessor(int threadCount, ThreadPriority threadPriority, bool isBlockingAdd, Func<TInput, TOutput> action) : base(new LambdaProcessor(action), threadCount, threadPriority, isBlockingAdd)
        {
        }

        private class LambdaProcessor : IProcessor<TInput, TOutput>
        {
            private readonly Func<TInput, TOutput> _action;

            public LambdaProcessor(Func<TInput, TOutput> action)
            {
                _action = action;
            }

            public TOutput Process(TInput input)
            {
                return _action(input);
            }
        }
    }
}
