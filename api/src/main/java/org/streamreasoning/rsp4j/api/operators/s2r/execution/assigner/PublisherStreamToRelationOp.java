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

import java.util.concurrent.Flow;

public abstract class PublisherStreamToRelationOp<E, O> implements Flow.Publisher<Content<E, O>>, StreamToRelationOp<E, O> {

    private static final Logger log = Logger.getLogger(PublisherStreamToRelationOp.class);
    protected final Ticker ticker;
    protected final Time time;
    protected final IRI iri;
    protected final ContentFactory<E, O> cf;
    protected Tick tick;
    protected ReportGrain grain;
    protected Report report;

    private HackySubscription subscription;

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
        this.subscription.setTimestamp(t_e);
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

    @Override
    public void subscribe(Flow.Subscriber<? super Content<E, O>> s) {
        this.subscription = new HackySubscription(s);
        s.onSubscribe(subscription);
    }
}
