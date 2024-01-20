import apache_beam as beam
import json
from apache_beam.io import WriteToBigQuery  # Import the missing module

def decode_message(msg):
    output = msg.decoded('utf-8')
    return json.loads(output)


# PubSub (PipelineOptions es una clase que permite configurar el pipeline)

from apache_beam.options.pipeline_options import PipelineOptions

with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    (
        p
        | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic='projects/phonic-obelisk-411712/topics/edem-topic')
        | 'Decode' >> beam.Map(decode_message)
        | 'Write to BigQuery' >> WriteToBigQuery(  # Correct the typo in the schema parameter
            table='phonic-obelisk-411712:edem_dataset.edem_table',
            schema="nombre:STRING",  # Correct the typo in the schema parameter
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            
        )
    )


