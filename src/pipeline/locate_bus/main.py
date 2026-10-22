from pipeline.locate_bus.transform.transform import run_transform
from pipeline.locate_bus.extract.extract import run_extract
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-e",   "--extract",    action="store_true", help="Extract data")
parser.add_argument("-t",   "--transform",  action="store_true", help="Transform data")
parser.add_argument("-d",   "--data",       action="store_true", help="Data for Run Transform")
# parser.add_argument("-l",   "--load",       action="store_true", help="Load data")
args = parser.parse_args()

if __name__ == "__main__":

    # Extract data
    if args.extract:
        run_extract()   # Collect data from RMTC API

    # Transform data
    if args.transform:
        run_transform(
            data=args.data \
            if args.data is not False \
            else ""
        )

    # # Load data
    # if args.load:
    #     transformed = None
