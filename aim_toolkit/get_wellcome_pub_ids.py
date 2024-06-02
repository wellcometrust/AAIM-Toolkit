import click
import pandas as pd
import awswrangler as wr
from rich.progress import track


def get_s3_uris(base_uri, start_year, end_year):
    """Generate parquet URIs for provided years.

    Follows internal URI naming schema.

    Args:
        base_uri(str): Base S3 URI path containing parquets.
        start_year(str): Earliest year in range.
        end_year(str): Most recent year in range.

    Returns:
        list: S3 URI strings.

    """
    uris = []
    for year in range(int(start_year), int(end_year) + 1):
        uris.append(f'{base_uri}year={year}/')
        uris.append(f'{base_uri}year={year}.0/')

    return uris


def read_parquet(uris, grid_id):
    """Read Wellcome DOIs from S3.

    Args:
        uris(list): S3 URIs of files to read.
        gird_id(str): Org grid ID to return DOIs for
            (default Wellcome Trust grid id).

    Returns:
        pd.DataFrame: Pandas data frame containing DOIs.

    """
    dfs = []
    for uri in track(uris):
        df = wr.s3.read_parquet(
            uri,
            columns=['doi', 'funding', 'pmid', 'pmcid']
        )

        df_funded = df.loc[df['funding'].str.len() > 0].explode('funding')
        funding = pd.DataFrame(df_funded['funding'].to_list())
        df_funded['grid_id'] = funding['grid_id']
        df_funded.loc[df_funded['grid_id'] == grid_id]
        df_funded.loc[~df_funded['pmid'].isna()]
        df_funded = df_funded[['doi', 'grid_id', 'pmid', 'pmcid']]
        dfs.append(df_funded)

    return pd.concat(dfs)


@click.command()
@click.argument("input_uri")
@click.argument("output_uri")
@click.argument("start_year")
@click.argument("end_year")
@click.option("--grid_id")
def get_org_dois(
    input_uri,
    output_uri,
    start_year,
    end_year,
    grid_id='grid.52788.30'
):
    """Retrieve publication DOIs for an organisation and save to S3.

    Args:
        input_uri(str): Base S3 URI path containing input parquets.
        output_uri(str): S3 location to save output data to.
        start_year(str): Earliest year in range.
        end_year(str): Most recent year in range.
        grid_id(str): Org grid ID to return DOIs for
            (default Wellcome Trust grid id).

    """
    s3_uris = get_s3_uris(input_uri, start_year, end_year)
    df = read_parquet(s3_uris, grid_id)
    wr.s3.to_parquet(df, output_uri, index=False)


if __name__ == '__main__':
    get_org_dois()
