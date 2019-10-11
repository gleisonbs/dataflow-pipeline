import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
'project': 'serious-mariner-255222',
    'staging_location': 'gs://mybucket20b40d8c/staging',
    'runner': 'DataflowRunner',
    'job_name': 'my-job-is-nameless-2t4tfw4e',
    'disk_size_gb': 100,
    'temp_location': 'gs://mybucket20b40d8c/temp',
    'save_main_session': True
}

options = PipelineOptions.from_dictionary(pipeline_options)
p = beam.Pipeline(options=options)

lines = (p
        | ReadFromText('gs://mybucket20b40d8c/data.csv')
        | WriteToText('gs://mybucket20b40d8c/my-data.csv'))

result = p.run()
result.wait_until_finish()
