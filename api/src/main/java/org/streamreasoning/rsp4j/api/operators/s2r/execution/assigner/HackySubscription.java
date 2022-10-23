package org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner;

import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

public class HackySubscription<E, O> implements Flow.Subscription {

        private long timestamp;
        private final Flow.Subscriber<? super Content<E, O>> subscriber;
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        public HackySubscription(Flow.Subscriber<? super Content<E, O>> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
//            if (n <= 0) {
//                subscriber.onError(new IllegalArgumentException());
//            }
//
//            for (long demand = n; demand >0 &&  !terminated.get(); demand--) {
//            }

//            if (!terminated.getAndSet(true)) {
//                subscriber.onComplete();
//            }
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long t_e) {
            this.timestamp = t_e;
        }

        @Override
        public void cancel() {
            terminated.set(true);
        }
}

