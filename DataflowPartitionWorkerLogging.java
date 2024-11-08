
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowPartitionWorkerLogging {
    private static final Logger LOG = LoggerFactory.getLogger(DataflowPartitionWorkerLogging.class);

    public static void main(String[] args) {
        // Create the pipeline options
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        // Get the job name (which includes the partition information)
        String jobName = options.getJobName();

        // Build and run the pipeline
        Pipeline pipeline = Pipeline.create(options);
        pipeline
            .apply("LogPartitionAndWorker", ParDo.of(new LogPartitionAndWorkerFn(jobName)))
            .apply("YourPipelineStep", /* your pipeline logic */);
        pipeline.run();
    }

    static class LogPartitionAndWorkerFn extends DoFn<String, String> {
        private final String jobName;

        public LogPartitionAndWorkerFn(String jobName) {
            this.jobName = jobName;
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            String workerName = context.getPipelineOptions().as(PipelineOptions.class).getWorkerName();
            LOG.info("Processing data from partition: {}, worker: {}", jobName, workerName);
            context.output(/* your pipeline data */);
        }
    }
}
