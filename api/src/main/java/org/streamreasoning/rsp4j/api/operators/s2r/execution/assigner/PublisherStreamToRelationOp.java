package org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner;

import org.apache.commons.rdf.api.IRI;
import org.apache.log4j.Logger;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.tick.Ticker;
import org.streamreasoning.rsp4j.api.secret.tick.secret.TickerFactory;
import org.streamreasoning.rsp4j.api.secret.time.Time;

import java.util.List;
import java.util.concurrent.*;

public abstract class PublisherStreamToRelationOp<E, O> implements StreamToRelationOp<E, O> {

    private static final Logger log = Logger.getLogger(PublisherStreamToRelationOp.class);
    protected final Ticker ticker;
    protected final Time time;
    protected final IRI iri;
    protected final ContentFactory<E, O> cf;
    protected Tick tick;
    protected ReportGrain grain;
    protected Report report;


    private List<S2RSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = ForkJoinPool.commonPool(); // daemon-based

    protected PublisherStreamToRelationOp(IRI iri, Time time, Tick tick, Report report, ReportGrain grain, ContentFactory<E, O> cf) {
        this.time = time;
        this.iri = iri;
        this.tick = tick;
        this.ticker = TickerFactory.tick(tick, this);
        this.report = report;
        this.grain = grain;
        this.cf = cf;
    }

    @Override
    public ReportGrain grain() {
        return grain;
    }


    @Override
    public void notify(E arg, long ts) {
        windowing(arg, ts);
    }

    @Override
    public Report report() {
        return report;
    }

    @Override
    public Tick tick() {
        return tick;
    }

    protected Content<E, O> setVisible(long t_e, Window w, Content<E, O> c) {
        log.debug("Report [" + w.getO() + "," + w.getC() + ") with Content " + c + "");
        return c;
    }

    public abstract void windowing(E arg, long ts);

    @Override
    public String iri() {
        return iri.getIRIString();
    }

    @Override
    public boolean named() {
        return iri != null;
    }

    public void submit(Long item) {

        for (S2RSubscription subscription : subscriptions) {
            if (!subscription.completed && subscription.demand > 0) {
                subscription.offer(item);
                subscription.demand -= 1;
            }
        }
    }

    public void subscribe(Flow.Subscriber subscriber) {
        S2RSubscription subscription = new S2RSubscription(subscriber, executor);

        subscriptions.add(subscription);
        subscriber.onSubscribe(subscription);
    }


    static class S2RSubscription implements Flow.Subscription {
        private final Flow.Subscriber subscriber;
        private final ExecutorService executor;
        volatile boolean completed = false;
        volatile int demand = 0;

        S2RSubscription(Flow.Subscriber subscriber,
                        ExecutorService executor) {
            this.subscriber = subscriber;
            this.executor = executor;
        }

        @Override
        public void request(long n) {
            if (n != 0 && !completed) {
                if (n < 0) {
                    IllegalArgumentException ex = new IllegalArgumentException();
                    executor.execute(() -> subscriber.onError(ex));
                } else {
                    // just extending the demand
                    demand += n;
                }
            }
        }

        @Override
        public void cancel() {
            completed = true;
        }

        void offer(Long item) {
            subscriber.onNext(item);
        }
    }
}
