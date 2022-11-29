"""
This script loads in contact centre popup data and invoice data from production server,
extracts document numbers from popup call entries, then attribute revenue generated to
each call based on 9-digit original document number
"""

import time
import logging
import pandas as pd

from ds_shared_lib.ms_sql import MSSQL
from helpers import logging_config
from logging.config import dictConfig


mssql = MSSQL()
dictConfig(logging_config)
logger = logging.getLogger()


def read_popup_data():
    df = mssql.read_to_df(
        """
        SELECT MeetingId
               ,PartsDocumentNumber
               ,Notes
        FROM   [Appdb_Equiplink].[datascience].[PartsCallCentre_Popup_Data]
        """,
        stored_proc=False,
        data_name='popup',
        pd_kwargs={'index_col': 'MeetingId'}
    )

    return df


def extract_doc_num(df):
    """Extract document numbers from PartsDocumentNumber and Notes columns"""
    # preprocess text columns that contain document numbers
    doc_num_cols = ['PartsDocumentNumber', 'Notes']
    for col in doc_num_cols:
        df.loc[:, col] = df.loc[:, col].str.strip().str.upper()

    # extract full & partial document numbers from PartsDocumentNumber
    doc_num_pat = r'\d{2}[A-Z]\d{6}'
    doc_num_mask = (df.loc[:, 'PartsDocumentNumber']
                      .str.match(f'{doc_num_pat}')
                      .fillna(False))

    pdn_extract = (df.loc[doc_num_mask, 'PartsDocumentNumber']
                     .str.extractall(fr'(?P<RootDocumentNumber>{doc_num_pat}|\b\d+)')
                     .sort_index()
                     .pipe(fix_doc_num, 'RootDocumentNumber'))

    # extract full document numbers from Notes
    notes_extract = (df.loc[:, 'Notes']
                       .str.extractall(fr'(?P<RootDocumentNumber>{doc_num_pat})'))

    # combine all document numbers and create sales type
    doc_num_df = (pd.concat([pdn_extract, notes_extract])
                    .drop_duplicates()
                    .droplevel('match')
                    .reset_index())

    return doc_num_df


def fix_doc_num(df, col, partial_mask_upper=6, full_len=9):
    """fix partial document numbers"""
    partial_mask = df[col].str.len() <= partial_mask_upper
    # In theory, # of digits in a partial doc num should not exceed 6,
    # e.g. 00C[074511], hence the default partial_mask_upper is set at
    # 6 to prevent extracting non-doc-num related digits

    for idx in df.loc[partial_mask, :].index.values:
        base_num = df.loc[idx[0], col].values[0]
        partial_num = df.loc[idx, col].values[0]
        missing_len = full_len - len(partial_num)
        fixed_num = f'{base_num[:missing_len]}{partial_num}'
        df.loc[idx, col] = fixed_num

    return df


def main(read=False, extract=False, write=False):
    start = time.perf_counter()
    block_sep = f"{'-' * 50}"

    if read:
        popup = read_popup_data()
        logger.info(block_sep)

        if extract:
            logger.info('Extracting document number...')

            result = extract_doc_num(popup)

            logger.info('Document number extraction complete.')
            logger.info(f"Sample parsed document number:\n{result.head()}")
            logger.info(block_sep)

            if write:
                mssql.write_to_table(
                    result,
                    batch_size=10000,
                    trunc_proc=None,
                    insert_proc='Appdb_EquipLink.datascience.PartsCallCentre_ParsedDocumentNumber_Insert',
                    import_proc=None
                )

                logger.info(block_sep)

        end = time.perf_counter()
        logger.info(f"Whole process took {end - start:0.2f} seconds.")
        logger.info(block_sep)


if __name__ == '__main__':
    main(read=True, extract=True, write=False)
