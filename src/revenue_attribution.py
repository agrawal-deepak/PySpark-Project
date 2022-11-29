"""
This script loads in contact centre popup data and invoice data from production server,
extracts document numbers from popup call entries, then attribute revenue generated to
each call based on 9-digit original document number
"""

import time
import logging
import pandas as pd

from ds_shared_lib.ms_sql import MSSQL
from helpers import logging_config, find_unique
from logging.config import dictConfig
from deprecation import deprecated

@deprecated(deprecated_in="2020/10/27", details="code converted to pyspark by mmada@lixar.com")

mssql = MSSQL()
dictConfig(logging_config)
logger = logging.getLogger()


def read_data():
    df = mssql.read_to_df(
        """
        SELECT d.MeetingId,
               d.RootDocumentNumber,
               i.PartSales,
               i.OpenDate,
               i.ContractStart,
               i.ContractEnd
        FROM Appdb_Equiplink.datascience.PartsCallCentre_ParsedDocumentNumber d
             LEFT JOIN Appdb_Equiplink.datascience.PartsCallCentre_Invoice_Data i
                    ON d.RootDocumentNumber = i.RootDocumentNumber
        """,
        stored_proc=False,
        pd_kwargs={'parse_dates': ['OpenDate', 'ContractStart', 'ContractEnd']}
    )

    return df


def aggregate_invoice(df):
    """Aggregate part sales based on root document number in invoice"""
    df = (df.groupby('RootDocumentNumber')
            .agg({'PartSales': 'sum',
                  'OpenDate': 'min',
                  'ContractStart': 'min',
                  'ContractEnd': 'max'})
            .eval('OnContract = OpenDate>=ContractStart & OpenDate<=ContractEnd')
            .astype({'OnContract': 'Int64'})
            .reset_index())

    return df


def attribute_revenue(df):
    """Merge document numbers with invoice data and calculate total part sales
    by meeting id"""
    df = (df.merge(aggregate_invoice(df), how='left')
            .groupby('MeetingId')
            .agg({'RootDocumentNumber': find_unique,
                  'PartSales': 'sum',
                  'OnContract': 'sum'})
            .assign(SalesType=lambda x: x.RootDocumentNumber.str.extract(r'([CS])')))

    return df


def main(read=False, process=False, write=False):
    start = time.perf_counter()
    block_sep = f"{'-' * 50}"

    if read:
        df = read_data()
        logger.info(block_sep)

        if process:
            logger.info('Attributing revenue to calls...')

            result = attribute_revenue(df)

            logger.info('Revenue attribution complete.')
            logger.info(f"Sample result table:\n{result.head()}")
            logger.info(block_sep)

            if write:
                mssql.write_to_table(
                    result,
                    batch_size=10000,
                    trunc_proc=None,
                    insert_proc='Appdb_EquipLink.datascience.PartsCallCentre_Revenue_Insert',
                    import_proc=None
                )
                logger.info(block_sep)

    end = time.perf_counter()
    logger.info(f"Whole process took {end - start:0.2f} seconds.")
    logger.info(block_sep)


if __name__ == '__main__':
    main(read=True, process=False, write=False)
