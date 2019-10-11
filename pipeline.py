import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    pipeline_options = {
        'project': 'serious-mariner-255222',
        'staging_location': 'gs://mybucket20b40d8c/staging',
        'temp_location': 'gs://mybucket20b40d8c/temp',
        'template_location': 'gs://mybucket20b40d8c/templates/number_lines_temp',
        'runner': 'DataflowRunner',
        'job_name': 'my-dataflow-job',
        'output': 'gs://mybucket20b40d8c/output/new-data.txt',
        'input': 'gs://mybucket20b40d8c/data.txt',
    }

    def remove_new_line(line):
        return line.strip('\n')

    def append_number(line):
        return f'1 - {line}'

    pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

    p = beam.Pipeline(options=pipeline_options)

    output = (p | 'read' >> ReadFromText('gs://mybucket20b40d8c/data.txt')
                | 'remove_new_lines' >> beam.Map(remove_new_line)
                | 'append_number' >> beam.Map(append_number)
                | 'write' >> WriteToText('gs://mybucket20b40d8c/output/new-data.txt'))

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()