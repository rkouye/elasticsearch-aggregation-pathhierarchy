package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Comparator;

public class DateHierarchyAggregator extends TermsAggregator {

    private final ValuesSource.Numeric valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketOrder order;
    private final long minDocCount;
    private final BucketCountThresholds bucketCountThresholds;
    private final List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo;

    public DateHierarchyAggregator(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource.Numeric valuesSource,
            BucketOrder order,
            long minDocCount,
            BucketCountThresholds bucketCountThresholds,
            List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo,
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, DocValueFormat.RAW,
                subAggCollectionMode(bucketCountThresholds.getShardSize()), pipelineAggregators, metaData);

        this.valuesSource = valuesSource;
        this.roundingsInfo = roundingsInfo;
        this.minDocCount = minDocCount;
        bucketOrds =  new BytesRefHash(1, context.bigArrays());
        this.order = InternalOrder.validate(order, this);
        this.bucketCountThresholds = bucketCountThresholds;
    }

    private static SubAggCollectionMode subAggCollectionMode(int shardSize){
        // We don't know the field cardinality, so we default to BF unless we know for sure all buckets will be returned
        // It is more performant and accuracy is a tradeoff of shard size just like TermsAggregation
        return shardSize == Integer.MAX_VALUE ? SubAggCollectionMode.DEPTH_FIRST : SubAggCollectionMode.BREADTH_FIRST;
    }

    /**
     * The collector collects the docs, including or not some score (depending of the including of a Scorer) in the
     * collect() process.
     *
     * The LeafBucketCollector is a "Per-leaf bucket collector". It collects docs for the account of buckets.
     */
    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        String path = "";
                        for (DateHierarchyAggregationBuilder.RoundingInfo roundingInfo: roundingsInfo) {
                            // A little hacky: Add a microsecond to avoid collision between min dates interval
                            // Since interval cannot be set to microsecond, it is not a problem
                            long roundedValue = roundingInfo.rounding.round(value);
                            path += roundingInfo.format.format(roundedValue).toString();
                            long bucketOrd = bucketOrds.add(new BytesRef(path));
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                            path += "/";
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        // build buckets and store them sorted
        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        PathSortedTree<String, InternalDateHierarchy.InternalBucket> pathSortedTree = new PathSortedTree<>(order.comparator(this), size);

        // Collect buckets
        long[] bucketOrdArray = new long[size];
        for (int i = 0; i < size; i++) {
            bucketOrdArray[i] = i;
        }
        runDeferredCollections(bucketOrdArray);

        InternalDateHierarchy.InternalBucket spare;
        for (int i = 0; i < size; i++) {
            spare = new InternalDateHierarchy.InternalBucket(0, null, null, null, 0, null);

            BytesRef term = new BytesRef();
            bucketOrds.get(i, term);
            String [] paths = term.utf8ToString().split("/", -1);

            spare.paths = paths;
            spare.key = term;
            spare.level = paths.length - 1;
            spare.name = paths[spare.level];
            spare.aggregations = bucketAggregations(i);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;

            pathSortedTree.add(spare.paths, spare);

            consumeBucketsAndMaybeBreak(1);

        }

        // Get the top buckets
        final List<InternalDateHierarchy.InternalBucket> list = new ArrayList<>(size);
        long otherHierarchyNodes = pathSortedTree.getFullSize();
        Iterator<InternalDateHierarchy.InternalBucket> iterator = pathSortedTree.consumer();
        for (int i = 0; i < size; i++) {
            final InternalDateHierarchy.InternalBucket bucket = iterator.next();
            list.add(bucket);
            otherHierarchyNodes -= 1;
        }

        return new InternalDateHierarchy(name, list, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), otherHierarchyNodes, pipelineAggregators(), metaData());
    }

    @Override
    public Comparator<MultiBucketsAggregation.Bucket> bucketComparator(AggregationPath path, boolean asc) {

        final Aggregator aggregator = path.resolveAggregator(this);
        final String key = path.lastPathElement().key;

        if (aggregator instanceof SingleBucketAggregator) {
            assert key == null : "this should be picked up before the aggregation is executed - on validate";
            return (b1, b2) -> {
                int mul = asc ? 1 : -1;
                int v1 = ((SingleBucketAggregator) aggregator).bucketDocCount(((InternalDateHierarchy.InternalBucket) b1).bucketOrd);
                int v2 = ((SingleBucketAggregator) aggregator).bucketDocCount(((InternalDateHierarchy.InternalBucket) b2).bucketOrd);
                return mul * (v1 - v2);
            };
        }

        // with only support single-bucket aggregators
        assert !(aggregator instanceof BucketsAggregator) : "this should be picked up before the aggregation is executed - on validate";

        if (aggregator instanceof NumericMetricsAggregator.MultiValue) {
            assert key != null : "this should be picked up before the aggregation is executed - on validate";
            return (b1, b2) -> {
                double v1 = ((NumericMetricsAggregator.MultiValue) aggregator)
                        .metric(key, ((InternalDateHierarchy.InternalBucket) b1).bucketOrd);
                double v2 = ((NumericMetricsAggregator.MultiValue) aggregator)
                        .metric(key, ((InternalDateHierarchy.InternalBucket) b2).bucketOrd);
                // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
                // the bottom
                return Comparators.compareDiscardNaN(v1, v2, asc);
            };
        }

        // single-value metrics agg
        return (b1, b2) -> {
            double v1 = ((NumericMetricsAggregator.SingleValue) aggregator)
                    .metric(((InternalDateHierarchy.InternalBucket) b1).bucketOrd);
            double v2 = ((NumericMetricsAggregator.SingleValue) aggregator)
                    .metric(((InternalDateHierarchy.InternalBucket) b2).bucketOrd);
            // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
            // the bottom
            return Comparators.compareDiscardNaN(v1, v2, asc);
        };
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalDateHierarchy(name, null, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), 0, pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
