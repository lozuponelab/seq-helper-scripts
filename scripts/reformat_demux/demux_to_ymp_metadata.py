import pandas as pd
import numpy as np
import argparse
import os


def get_args():
    """
    handles arg parsing for this script

    returns the parsed args
    """
    parser = argparse.ArgumentParser(
        prog="Demux to metadata",
        description="Converts Anschutz Genomics Core demux files to YMP metadata"
    )

    parser.add_argument("-i", "--infile",
                        required=True)
    parser.add_argument("-o", "--outfile",
                        required=True)
    parser.add_argument("-d", "--directory",
                        required=True)
    parser.add_argument("-s", "--skipnrows",
                        help="Sometimes this should be 4, 5, or 6. It seems like they change this every time they sequence.",
                        required=False,
                        default=5, type=int)

    parsed_args = parser.parse_args()
    return parsed_args


def sample_id_to_files(dir, sampleid, read_sig):
    """
    :param dir: str: directory to search
    :param sampleid: str: sampleid to look for
    :param read_sig: str: signifier to also look for (e.g., "R1" for fwd read)
    :return: str: filename
    """
    for file in os.listdir(dir):
        if file.startswith(sampleid) and read_sig in file:
            # this assumes you're running the file from where you want
            # the ymp metadata to be
            # path will be relative to where this script is run from
            return os.path.join(dir, file)

    # if no file returned
    return np.nan


def main():
    args = get_args()

    df = pd.read_csv(args.infile,
                     skiprows=args.skipnrows).drop(0, axis=0)

    print(df)
    df["ForwardReads"] = df["Sample"].apply(
        lambda x: sample_id_to_files(args.directory, x, "R1")
    )
    df["ReverseReads"] = df["Sample"].apply(
        lambda x: sample_id_to_files(args.directory, x, "R2")
    )

    # YMP can't handle samples that don't have corresponding files
    # So if any files have been dropped from what's in the demux,
    # those are cut out here
    df = df.dropna(axis=0,
                   how="any",
                   subset=["ForwardReads", "ReverseReads"])

    order = ["Sample", "ForwardReads", "ReverseReads"]
    order = order + [col for col in df.columns if col not in order]

    new_df = df[order]

    print("Head of output file:")
    print(new_df.head())

    new_df.to_csv(args.outfile, index=False)


if __name__=="__main__":
    main()