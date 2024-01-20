import apache_beam as beam
import json
from apache_beam.io import WriteToBigQuery  # Import the missing module

def decode_message(msg):
    output = msg.decode('utf-8')
    return json.loads(output)

# PubSub (PipelineOptions es una clase que permite configurar el pipeline)

from apache_beam.options.pipeline_options import PipelineOptions

with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    (
        p
        | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription='projects/phonic-obelisk-411712/subscriptions/edem-topic-sub')
        | 'Decode' >> beam.Map(decode_message)
        | 'Write to BigQuery' >> WriteToBigQuery(  # Correct the typo in the schema parameter
            table='phonic-obelisk-411712:dataset.edem_tabla',
            schema="nombre:STRING",  # Correct the typo in the schema parameter
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            
        )
    )
