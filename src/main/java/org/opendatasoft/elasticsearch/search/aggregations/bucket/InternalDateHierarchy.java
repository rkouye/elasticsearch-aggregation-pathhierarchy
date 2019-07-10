package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An internal implementation of {@link InternalMultiBucketAggregation}
 * which extends {@link org.elasticsearch.search.aggregations.Aggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 */
public class InternalDateHierarchy extends InternalMultiBucketAggregation<InternalDateHierarchy,
        InternalDateHierarchy.InternalBucket> implements MultiBucketAggregationBuilder {

    /**
     * The bucket class of InternalDateHierarchy.
     * @see MultiBucketsAggregation.Bucket
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements
            MultiBucketsAggregation.Bucket, KeyComparable<InternalBucket> {

        long key;
        protected Long[] paths;
        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected DocValueFormat format;

        public InternalBucket(long docCount, InternalAggregations aggregations, long key, int level, Long[] paths, DocValueFormat format) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.paths = paths;
            this.format = format;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            key = in.readLong();
            docCount = in.readLong();
            aggregations = InternalAggregations.readAggregations(in);
            level = in.readInt();
            format = in.readNamedWriteable(DocValueFormat.class);
            int pathsSize = in.readInt();
            paths = new Long[pathsSize];
            for (int i=0; i < pathsSize; i++) {
                paths[i] = in.readLong();
            }
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
            out.writeLong(docCount);
            aggregations.writeTo(out);
            out.writeInt(level);
            out.writeNamedWriteable(format);
            out.writeInt(paths.length);
            for (Long path: paths) {
                out.writeLong(path);
            }
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return format.format(key).toString();
        }

        @Override
        public int compareKey(InternalBucket other) {
            return Long.compare(key, other.key);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        /**
         * Utility method of InternalDateHierarchy.doReduce()
         */
        InternalBucket reduce(List<InternalBucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            InternalBucket reduced = null;
            for (InternalBucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
            return reduced;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }


    private List<InternalBucket> buckets;
    private BucketOrder order;
    private final int requiredSize;
    private final int shardSize;
    private final long otherHierarchyNodes;
    private final long minDocCount;

    public InternalDateHierarchy(
            String name,
            List<InternalBucket> buckets,
            BucketOrder order,
            long minDocCount,
            int requiredSize,
            int shardSize,
            long otherHierarchyNodes,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        this.minDocCount = minDocCount;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.otherHierarchyNodes = otherHierarchyNodes;
    }

    /**
     * Read from a stream.
     */
    public InternalDateHierarchy(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        minDocCount = in.readVLong();
        requiredSize = readSize(in);
        shardSize = readSize(in);
        otherHierarchyNodes = in.readVLong();
        int bucketsSize = in.readInt();
        this.buckets = new ArrayList<>(bucketsSize);
        for (int i=0; i<bucketsSize; i++) {
            this.buckets.add(new InternalBucket(in));
        }
    }

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVLong(minDocCount);
        writeSize(requiredSize, out);
        writeSize(shardSize, out);
        out.writeVLong(otherHierarchyNodes);
        out.writeInt(buckets.size());
        for (InternalBucket bucket: buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return DateHierarchyAggregationBuilder.NAME;
    }

    protected int getShardSize() {
        return shardSize;
    }

    public long getSumOtherHierarchyNodes() {
        return otherHierarchyNodes;
    }

    @Override
    public InternalDateHierarchy create(List<InternalBucket> buckets) {
        return new InternalDateHierarchy(
                this.name, buckets, order, minDocCount, requiredSize, shardSize, otherHierarchyNodes,
                this.pipelineAggregators(), this.metaData);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.docCount, aggregations, prototype.key, prototype.level, prototype.paths, prototype.format);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    /**
     * Reduces the given aggregations to a single one and returns it.
     */
    @Override
    public InternalDateHierarchy doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Long, List<InternalBucket>> buckets = null;
        long otherHierarchyNodes = 0;

        // extract buckets from aggregations
        for (InternalAggregation aggregation : aggregations) {
            InternalDateHierarchy dateHierarchy = (InternalDateHierarchy) aggregation;
            if (buckets == null) {
                buckets = new LinkedHashMap<>();
            }

            otherHierarchyNodes += dateHierarchy.getSumOtherHierarchyNodes();

            for (InternalBucket bucket : dateHierarchy.buckets) {
                List<InternalBucket> existingBuckets = buckets.get(bucket.key);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.key, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        // reduce and sort buckets depending of ordering rules
        final int size = !reduceContext.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size());
        PathSortedTree<Long, InternalBucket> ordered = new PathSortedTree<>(order.comparator(null), size);
        for (List<InternalBucket> sameTermBuckets : buckets.values()) {

            final InternalBucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.getDocCount() >= minDocCount || !reduceContext.isFinalReduce()) {
                reduceContext.consumeBucketsAndMaybeBreak(1);
                ordered.add(b.paths, b);
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(b));
            }
        }

        long sum_other_hierarchy_nodes = ordered.getFullSize() - size + otherHierarchyNodes;
        return new InternalDateHierarchy(getName(), ordered.getAsList(), order, minDocCount, requiredSize, shardSize,
                sum_other_hierarchy_nodes, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        Iterator<InternalBucket> bucketIterator = buckets.iterator();
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        InternalBucket prevBucket = null;
        InternalBucket currentBucket = null;
        while (bucketIterator.hasNext()) {
            currentBucket = bucketIterator.next();

            if (prevBucket != null) {
                if (prevBucket.level == currentBucket.level) {
                    builder.endObject();
                } else if (prevBucket.level < currentBucket.level) {
                    builder.startObject(name);
                    builder.startArray(CommonFields.BUCKETS.getPreferredName());
                } else {
                    for (int i = currentBucket.level; i < prevBucket.level; i++) {
                        builder.endObject();
                        builder.endArray();
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }

            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), currentBucket.getKeyAsString());
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), currentBucket.docCount);
            currentBucket.getAggregations().toXContentInternal(builder, params);

            prevBucket = currentBucket;
        }

        if (currentBucket != null) {
            for (int i=0; i < currentBucket.level; i++) {
                builder.endObject();
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
        }

        builder.endArray();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, order, requiredSize, shardSize, otherHierarchyNodes, minDocCount);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalDateHierarchy that = (InternalDateHierarchy) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(order, that.order)
                && Objects.equals(minDocCount, that.minDocCount)
                && Objects.equals(requiredSize, that.requiredSize)
                && Objects.equals(shardSize, that.shardSize)
                && Objects.equals(otherHierarchyNodes, that.otherHierarchyNodes);
    }
}
