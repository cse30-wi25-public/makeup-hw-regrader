import logging
import argparse
from request import PLAPI
import csv

logging.basicConfig(filename="pl.log", level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pl")

def write_to_csv(filename, results):
    with open(f"{filename}", mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        cols = next(iter(results.values())).keys()
        writer.writerow(cols)
        for _, v in results.items():
            writer.writerow(
                v.values()
            )

    print(f"Total score saved to {filename}")



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--domain", help="PrairieLearn host", default="https://us.prairielearn.com")
    parser.add_argument("-o", "--output", help="Output filename", default="grade.csv")
    parser.add_argument("-t", "--token", help="PrairieLearn token", required=True)
    parser.add_argument("-c", "--course", help="Course instance ID", required=True)

    args = parser.parse_args()

    DOMAIN = args.domain
    TOKEN = args.token
    COURSE_INSTANCE_ID = args.course
    filename = args.output

    api = PLAPI(DOMAIN, TOKEN, COURSE_INSTANCE_ID)

    assmt_id = api.get_assmt()
    results = api.fetch_grade(assmt_id)
    write_to_csv(filename, results)


if __name__ == "__main__":
    main()
