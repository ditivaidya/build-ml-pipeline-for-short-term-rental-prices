#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info("Cleaning data")
    # Drop outliers
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Drop rows in the dataset that are not in the proper geolocation
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Save file to csv 
    logger.info(f"Save {args.output_artifact_desc} as .csv")
    df.to_csv(args.output_artifact_name, index=False)

    # Upload Artifact
    logger.info(f"Uploading {args.output_artifact_name} file to W&B")
    artifact = wandb.Artifact(
        args.output_artifact_name,
        type=args.output_artifact_type,
        description=args.output_artifact_desc,)
    
    artifact.add_file(args.output_artifact_name)
    run.log_artifact(artifact)


    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input csv data file name (eg:'sample.csv:latest')",
        required=True
    )

    parser.add_argument(
        "--output_artifact_name", 
        type=str,
        help="Name of cleaned file saved as .csv file (eg: 'clean_data.csv')",
        required=True
    )

    parser.add_argument(
        "--output_artifact_type", 
        type=str,
        help="type of artifact file (eg: 'clean_data')",
        required=True
    )

    parser.add_argument(
        "--output_artifact_desc", 
        type=str,
        help="description of output file generated",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Price lower limit (eg: 10)",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Price upper limit (eg: 350)",
        required=True
    )


    args = parser.parse_args()

    go(args)
