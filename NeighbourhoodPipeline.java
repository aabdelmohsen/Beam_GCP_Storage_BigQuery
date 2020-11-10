/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.Arrays;

public class NeighbourhoodPipeline {
	public interface NeighbourhoodOptions extends PipelineOptions {

		@Description("Path of the file to read from")
		@Default.String("gs://springml_gcp_bucket/newyork-airbnb/AB_NYC_2019.csv")
		String getInputFile();

		void setInputFile(String value);

		/** Set this required option to specify where to write the output. */
		@Description("Path of the file to write to")
		@Required
		String getOutput();

		void setOutput(String value);
	}

	static class CsvCommons extends DoFn<ReadableFile, CSVRecord> {
		@DoFn.ProcessElement
		public void processElement(@Element ReadableFile element, DoFn.OutputReceiver<CSVRecord> receiver)
				throws IOException {
			InputStream is = Channels.newInputStream(element.open());
			Reader reader = new InputStreamReader(is);
			Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);

			for (CSVRecord record : records) {
				receiver.output(record);
			}
		}
	}

	public static void main(String[] args) {
		String projectId = "newyork-airbnb";
		String dataset = "springml_task_dataset";
		final Logger LOG = LoggerFactory.getLogger(NeighbourhoodPipeline.class);

		try {

			NeighbourhoodOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
					.as(NeighbourhoodOptions.class);
			options.setJobName("NeighbourhoodPipeline");
			Pipeline pipeline = Pipeline.create(options);

			PCollection<CSVRecord> records = pipeline.apply(FileIO.match().filepattern(options.getInputFile())) // PCollection<Metadata>
					.apply(FileIO.readMatches()) // PCollection<ReadableFile>
					.apply(ParDo.of(new CsvCommons())) // PCollection<CSVRecord>
			;

			PCollection<String> neighbourhoods = records.apply(MapElements.via(new SimpleFunction<CSVRecord, String>() {
				@Override
				public String apply(CSVRecord input) {
					return input.get(5);
				}
			}));

			PCollection<KV<String, Long>> neighbourhoodCounts = neighbourhoods.apply(Count.perElement());
			PCollection<TableRow> rows = neighbourhoodCounts
					.apply(MapElements.via(new SimpleFunction<KV<String, Long>, TableRow>() {
						@Override
						public TableRow apply(KV<String, Long> input) {
							return new TableRow().set("name", input.getKey()).set("count", input.getValue().intValue());
						}
					}));

			PCollection<TableRow> newyorkSchemaRecords = records
					.apply(MapElements.via(new SimpleFunction<CSVRecord, TableRow>() {
						@Override
						public TableRow apply(CSVRecord record) {

							return new TableRow()
									.set("id", (!record.get(0).isEmpty()) ? Integer.valueOf(record.get(0)) : null)
									.set("name", record.get(1)).set("host_id", record.get(2))
									.set("host_name", record.get(3)).set("neighbourhood_group", record.get(4))
									.set("neighbourhood", record.get(5))
									.set("latitude", (!record.get(6).isEmpty()) ? Float.valueOf(record.get(6)) : null)
									.set("longitude", (!record.get(7).isEmpty()) ? Float.valueOf(record.get(7)) : null)
									.set("room_type", record.get(8))
									.set("price", (!record.get(9).isEmpty()) ? Integer.valueOf(record.get(9)) : null)
									.set("minimum_nights",
											(!record.get(10).isEmpty()) ? Integer.valueOf(record.get(10)) : null)
									.set("number_of_reviews",
											(!record.get(11).isEmpty()) ? Integer.valueOf(record.get(11)) : null)
									.set("last_review", record.get(12))
									.set("reviews_per_month",
											(!record.get(13).isEmpty()) ? Float.valueOf(record.get(13)) : null)
									.set("calculated_host_listings_count",
											(!record.get(14).isEmpty()) ? Integer.valueOf(record.get(14)) : null)
									.set("availability_365",
											(!record.get(15).isEmpty()) ? Integer.valueOf(record.get(15)) : null);
						}
					}));

			TableSchema tableSchema = createPipelineResultSchema();
			writeToTable(projectId, dataset, "neighbourhood_count", tableSchema, rows);

			TableSchema newyorkAirbnbSchema = createNewyorkAirbnbSchema();
			writeToTable(projectId, dataset, "newyork_airbnb", newyorkAirbnbSchema, newyorkSchemaRecords);

			pipeline.run().waitUntilFinish();

		} catch (Exception e) {
			LOG.debug(e.getMessage());
			e.printStackTrace();
		}
	}

	public static void writeToTable(String project, String dataset, String table, TableSchema schema,
			PCollection<TableRow> rows) {

		ValueProvider<String> customGcsTempLocation;
		rows.apply("Write to BigQuery", BigQueryIO.writeTableRows()
				.to(String.format("%s:%s.%s", project, dataset, table)).withSchema(schema)
				// For CreateDisposition:
				// - CREATE_IF_NEEDED (default): creates the table if it doesn't exist, a schema
				// - CREATE_NEVER: raises an error if the table doesn't exist, a schema is not needed
				.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
				// For WriteDisposition:
				// - WRITE_EMPTY (default): raises an error if the table is not empty
				// - WRITE_APPEND: appends new rows to existing rows
				// - WRITE_TRUNCATE: deletes the existing rows before writing
				.withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));

	}

	public static TableSchema createPipelineResultSchema() {
		// neighbourhood group by result schema
		TableSchema schema = new TableSchema()
				.setFields(Arrays.asList(new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"),
						new TableFieldSchema().setName("count").setType("INT64").setMode("NULLABLE")));
		return schema;
	}

	public static TableSchema createNewyorkAirbnbSchema() {
		// Newyork Airbnb original data schema
		TableSchema schema = new TableSchema()
				.setFields(Arrays.asList(new TableFieldSchema().setName("id").setType("INT64"),
						new TableFieldSchema().setName("name").setType("STRING"),
						new TableFieldSchema().setName("host_id").setType("STRING"),
						new TableFieldSchema().setName("host_name").setType("STRING"), // default mode is "NULLABLE"
						new TableFieldSchema().setName("neighbourhood_group").setType("STRING"),
						new TableFieldSchema().setName("neighbourhood").setType("STRING"),
						new TableFieldSchema().setName("latitude").setType("FLOAT64"),
						new TableFieldSchema().setName("longitude").setType("FLOAT64"),
						new TableFieldSchema().setName("room_type").setType("STRING"),
						new TableFieldSchema().setName("price").setType("INT64"),
						new TableFieldSchema().setName("minimum_nights").setType("INT64"),
						new TableFieldSchema().setName("number_of_reviews").setType("INT64"),
						new TableFieldSchema().setName("last_review").setType("STRING"),
						new TableFieldSchema().setName("reviews_per_month").setType("FLOAT64"),
						new TableFieldSchema().setName("calculated_host_listings_count").setType("INT64"),
						new TableFieldSchema().setName("availability_365").setType("INT64")));

		return schema;
	}
}
