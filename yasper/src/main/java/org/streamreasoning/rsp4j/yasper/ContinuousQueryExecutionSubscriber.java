package org.streamreasoning.rsp4j.yasper;

import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import org.streamreasoning.rsp4j.api.sds.SDS;

import java.util.concurrent.Flow;

/**
 * Created by riccardo on 03/07/2017.
 */
public abstract class ContinuousQueryExecutionSubscriber<I, W, R, O> implements Flow.Subscriber, ContinuousQueryExecution<I, W, R, O> {

    protected ContinuousQuery query;
    protected RelationToStreamOperator s2r;
    protected SDS sds;
    protected Flow.Subscription subscription;

    public ContinuousQueryExecutionSubscriber(SDS sds, ContinuousQuery query) {
        this.query = query;
        this.sds = sds;
    }

    public ContinuousQueryExecutionSubscriber(ContinuousQuery query, RelationToStreamOperator s2r, SDS sds) {
        this.query = query;
        this.s2r = s2r;
        this.sds = sds;
    }
}
