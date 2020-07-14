package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PathHierarchyAggregator extends TermsAggregator {
    private final ValuesSource valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketOrder order;
    private final long minDocCount;
    private final int minDepth;
    private final BucketCountThresholds bucketCountThresholds;
    private final BytesRef separator;

    public PathHierarchyAggregator(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource valuesSource,
            BucketOrder order,
            long minDocCount,
            BucketCountThresholds bucketCountThresholds,
            BytesRef separator,
            int minDepth,
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, DocValueFormat.RAW,
                subAggCollectionMode(bucketCountThresholds.getShardSize()), pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.separator = separator;
        this.minDocCount = minDocCount;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.order = InternalOrder.validate(order, this);
        this.bucketCountThresholds = bucketCountThresholds;
        this.minDepth = minDepth;
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
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();
            /**
             * Collect the given doc in the given bucket.
             * Called once for every document matching a query, with the unbased document number.
             */
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                assert owningBucketOrdinal == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    previous.clear();

                    // SortedBinaryDocValues don't guarantee uniqueness so we need to take care of dups
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytesValue = values.nextValue();
                        if (i > 0 && previous.get().equals(bytesValue)) {
                            continue;
                        }
                        long bucketOrdinal = bucketOrds.add(bytesValue);
                        if (bucketOrdinal < 0) { // already seen
                            bucketOrdinal = - 1 - bucketOrdinal;
                            collectExistingBucket(sub, doc, bucketOrdinal);
                        } else {
                            collectBucket(sub, doc, bucketOrdinal);
                        }
                        previous.copyBytes(bytesValue);
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

        PathSortedTree<String, InternalPathHierarchy.InternalBucket> pathSortedTree = new PathSortedTree<>(order.comparator(this), size);

        // Collect buckets
        long[] bucketOrdArray = new long[size];
        for (int i = 0; i < size; i++) {
            bucketOrdArray[i] = i;
        }
        runDeferredCollections(bucketOrdArray);

        InternalPathHierarchy.InternalBucket spare;
        for (int i = 0; i < size; i++) {
            spare = new InternalPathHierarchy.InternalBucket(0, null, null, new BytesRef(), 0, 0, null);
            BytesRef term = new BytesRef();
            bucketOrds.get(i, term);

            String quotedPattern = Pattern.quote(separator.utf8ToString());

            String [] paths = term.utf8ToString().split(quotedPattern, -1);

            String [] pathsForTree;

            if (minDepth > 0) {
                pathsForTree = Arrays.copyOfRange(paths, minDepth, paths.length);
            } else {
                pathsForTree = paths;
            }

            spare.termBytes = BytesRef.deepCopyOf(term);
            spare.aggregations = bucketAggregations(i);
            spare.level = pathsForTree.length - 1;
            spare.docCount = bucketDocCount(i);
            spare.basename = paths[paths.length - 1];
            spare.minDepth = minDepth;
            spare.bucketOrd = i;
            spare.paths = paths;

            pathSortedTree.add(pathsForTree, spare);

            consumeBucketsAndMaybeBreak(1);
        }

        // Get the top buckets
        final List<InternalPathHierarchy.InternalBucket> list = new ArrayList<>(size);
        long otherHierarchyNodes = pathSortedTree.getFullSize();
        Iterator<InternalPathHierarchy.InternalBucket> iterator = pathSortedTree.consumer();
        for (int i = 0; i < size; i++) {
            final InternalPathHierarchy.InternalBucket bucket = iterator.next();
            list.add(bucket);
            otherHierarchyNodes -= 1;
        }

        return new InternalPathHierarchy(name, list, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), otherHierarchyNodes, separator, pipelineAggregators(), metaData());
    }

    @Override
    public Comparator<MultiBucketsAggregation.Bucket> bucketComparator(AggregationPath path, boolean asc) {

        final Aggregator aggregator = path.resolveAggregator(this);
        final String key = path.lastPathElement().key;

        if (aggregator instanceof SingleBucketAggregator) {
            assert key == null : "this should be picked up before the aggregation is executed - on validate";
            return (b1, b2) -> {
                int mul = asc ? 1 : -1;
                int v1 = ((SingleBucketAggregator) aggregator).bucketDocCount(((InternalPathHierarchy.InternalBucket) b1).bucketOrd);
                int v2 = ((SingleBucketAggregator) aggregator).bucketDocCount(((InternalPathHierarchy.InternalBucket) b2).bucketOrd);
                return mul * (v1 - v2);
            };
        }

        // with only support single-bucket aggregators
        assert !(aggregator instanceof BucketsAggregator) : "this should be picked up before the aggregation is executed - on validate";

        if (aggregator instanceof NumericMetricsAggregator.MultiValue) {
            assert key != null : "this should be picked up before the aggregation is executed - on validate";
            return (b1, b2) -> {
                double v1 = ((NumericMetricsAggregator.MultiValue) aggregator)
                        .metric(key, ((InternalPathHierarchy.InternalBucket) b1).bucketOrd);
                double v2 = ((NumericMetricsAggregator.MultiValue) aggregator)
                        .metric(key, ((InternalPathHierarchy.InternalBucket) b2).bucketOrd);
                // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
                // the bottom
                return Comparators.compareDiscardNaN(v1, v2, asc);
            };
        }

        // single-value metrics agg
        return (b1, b2) -> {
            double v1 = ((NumericMetricsAggregator.SingleValue) aggregator)
                    .metric(((InternalPathHierarchy.InternalBucket) b1).bucketOrd);
            double v2 = ((NumericMetricsAggregator.SingleValue) aggregator)
                    .metric(((InternalPathHierarchy.InternalBucket) b2).bucketOrd);
            // some metrics may return NaN (eg. avg, variance, etc...) in which case we'd like to push all of those to
            // the bottom
            return Comparators.compareDiscardNaN(v1, v2, asc);
        };
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPathHierarchy(name, null, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), 0, separator, pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
