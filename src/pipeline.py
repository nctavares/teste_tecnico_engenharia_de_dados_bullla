import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone
from unidecode import unidecode
import csv
import io
import pyarrow as pa


def parse_csv_to_dict(csv_line, header):
    csv_reader = csv.reader(io.StringIO(csv_line))
    return dict(zip(header, next(csv_reader)))

def preprocess_data(element):
    processed_element = {}
    for key, value in element.items():
        if isinstance(value, str):
            processed_value = unidecode(value).strip().upper()
            processed_element[key] = processed_value
        else:
            processed_element[key] = value
    return processed_element

def add_load_timestamp(element):
    element['DT_CARGA'] = datetime.now(timezone.utc).isoformat()
    return element


def run():
    options = PipelineOptions()

    header_q1 = ['nome', 'cpf', 'email']
    schema_q1 = pa.schema([
        pa.field('nome', pa.string()),
        pa.field('cpf', pa.string()),
        pa.field('email', pa.string()),
        pa.field('DT_CARGA', pa.string())
    ])
    
    with beam.Pipeline(options=options) as p1:
        (
            p1
            | 'Ler CSV da Query 1' >> beam.io.ReadFromText('data/query1_data.csv', skip_header_lines=1)
            | 'Parsear para Dicionário Q1' >> beam.Map(parse_csv_to_dict, header=header_q1)
            | 'Remover Duplicados Q1' >> beam.Distinct()
            | 'Pré-processar Dados Q1' >> beam.Map(preprocess_data)
            | 'Adicionar Timestamp Q1' >> beam.Map(add_load_timestamp)
            | 'Salvar Q1 em Parquet' >> beam.io.WriteToParquet(
                'data/query1_data',
                schema=schema_q1,
                file_name_suffix='.parquet',
                num_shards=1,
                shard_name_template=''
            )
        )

    header_q2 = ['cpf', 'numeroConta', 'numeroCartao', 'Ranking']
    schema_q2 = pa.schema([
        pa.field('cpf', pa.string()),
        pa.field('numeroConta', pa.string()),
        pa.field('numeroCartao', pa.string()),
        pa.field('Ranking', pa.string()),
        pa.field('DT_CARGA', pa.string())
    ])

    with beam.Pipeline(options=options) as p2:
        (
            p2
            | 'Ler CSV da Query 2' >> beam.io.ReadFromText('data/query2_data.csv', skip_header_lines=1)
            | 'Parsear para Dicionário Q2' >> beam.Map(parse_csv_to_dict, header=header_q2)
            | 'Converter para Tupla Q2' >> beam.Map(lambda element: tuple(sorted(element.items())))
            | 'Remover Duplicados Q2' >> beam.Distinct()
            | 'Converter para Dicionário Q2' >> beam.Map(lambda element: dict(element))
            | 'Pré-processar Dados Q2' >> beam.Map(preprocess_data)
            | 'Adicionar Timestamp Q2' >> beam.Map(add_load_timestamp)
            | 'Salvar Q2 em Parquet' >> beam.io.WriteToParquet(
                'data/query2_data',
                schema=schema_q2,
                file_name_suffix='.parquet',
                num_shards=1,
                shard_name_template=''
            )
        )

if __name__ == '__main__':
    run()
