# ++
import sys

from accelerators.databricks.scripts import download_and_unzip_direct_data_files, extract_doc_content
from accelerators.databricks.scripts import direct_data_to_object_storage
from accelerators.databricks.scripts import load_data
from accelerators.databricks.services.databricks_service import DatabricksService
from common.utilities import read_json_file

sys.path.append('.')
from common.services.aws_s3_service import AwsS3Service
from common.services.vault_service import VaultService


def main():
    config_filepath: str = "path/to/connector_config.json"
    vapil_settings_filepath: str = "path/to/vapil_settings.json"

    config_params: dict = read_json_file(config_filepath)
    convert_to_parquet: bool = config_params['convert_to_parquet']
    extract_document_content: bool = config_params['extract_document_content']

    direct_data_params: dict = config_params['direct_data']
    s3_params: dict = config_params['s3']
    databricks_params: dict = config_params['databricks']

    vault_service: VaultService = VaultService(vapil_settings_filepath)
    s3_service: AwsS3Service = AwsS3Service(s3_params)
    databricks_service: DatabricksService = DatabricksService(databricks_params)

    # TODO need to read below lines
    direct_data_to_object_storage.run(vault_service=vault_service,
                                      s3_service=s3_service,
                                      direct_data_params=direct_data_params)

    download_and_unzip_direct_data_files.run(s3_service=s3_service,
                                             convert_to_parquet=convert_to_parquet)

    load_data.run(s3_service=s3_service,
                  databricks_service=databricks_service,
                  direct_data_params=direct_data_params,
                  convert_to_parquet=convert_to_parquet)
    if extract_document_content:
        extract_doc_content.run(s3_service=s3_service,
                                 vault_service=vault_service,
                                 convert_to_parquet=convert_to_parquet)


if __name__ == "__main__":
    main()
