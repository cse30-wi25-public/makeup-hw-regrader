import requests
import logging
import time
import json
from urllib.parse import urljoin
from datetime import datetime, timedelta
import math
import concurrent.futures
import csv
import argparse
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--domain", help="PrairieLearn host", default="https://us.prairielearn.com")
parser.add_argument("-t", "--token", help="PrairieLearn token", required=True)
parser.add_argument("-c", "--course", help="Course instance ID", required=True)
args = parser.parse_args()
DOMAIN = args.domain
TOKEN = args.token
COURSE_INSTANCE_ID = args.course

server = urljoin(DOMAIN, "/pl/api/v1")

logging.basicConfig(filename="pl.log", level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pl")


def api_request(endpoint):
    logger.info(f"start request to '{endpoint}'")
    url = server + endpoint
    retry_max = 5
    retry_cnt = 0
    start_time = time.time()
    while True:
        response = requests.get(url, headers={"Private-Token": TOKEN})
        if response.status_code == 200:
            break
        elif response.status_code == 502:
            retry_cnt += 1
            if retry_cnt >= retry_max:
                logger.error(f"Maximum number of retries reached on 502 Bad Gateway Error for {url}")
                raise Exception(f"Maximum number of retries reached on 502 Bad Gateway Error for {url}")
            else:
                logger.info(f"Bad Gateway Error encountered for {url}, retrying in 10 seconds")
                time.sleep(10)
        else:
            logger.error(f"Invalid status returned for {url}: {response.status_code}")
            raise Exception(f"Invalid status returned for {url}: {response.status_code}")
    end_time = time.time()
    logger.info(
        f"request to '{endpoint}' {response.headers.get('content-length', -1)} bytes completed in {end_time - start_time:.2f} seconds"
    )

    return json.loads(response.text)


def get_assmt_id():
    assmts = api_request(f"/course_instances/{COURSE_INSTANCE_ID}/assessments")
    name2id = dict()
    for assmt in assmts:
        id = assmt["assessment_id"]
        name = assmt["assessment_name"]
        name2id[str(name)] = id
        name2id[str(id)] = id
        print(f"id: {id}, name: {name}")
    while True:
        id = input("Enter the assessment (either name or id): ")
        if id in name2id:
            break
        print("Invalid assessment name or id")
    logger.info(f"Selected assessment: {id}")
    return name2id[id]


def get_assmt_due_date(assmt_id):
    endpoint = f"/course_instances/{COURSE_INSTANCE_ID}/assessments/{assmt_id}/assessment_access_rules"
    access_rules = api_request(endpoint)
    due_date = max(
        [datetime.fromisoformat(t["end_date"]) for t in access_rules if t["mode"] == "Public" and t["credit"] == 100]
    )
    return due_date


def get_assmt_instances(assmt_id):
    endpoint = f"/course_instances/{COURSE_INSTANCE_ID}/assessments/{assmt_id}/assessment_instances"
    instances = api_request(endpoint)
    uid2id = {item["user_uid"]: item["assessment_instance_id"] for item in instances}
    return uid2id


def get_grade(uid, assmt_instance_id, due_date, makeup_due_date):
    log = api_request(f"/course_instances/{COURSE_INSTANCE_ID}/assessment_instances/{assmt_instance_id}/log")
    filtered_log = [
        event
        for event in log
        if event["event_name"] == "Score question" or event["event_name"] == "Submission"
    ]

    # assumption: manual grading will only ever happen for single-variant problems, unclear how to resolve otherwise
    last_submission_time_for_question = dict()
    for event in filtered_log:
        quid = event["question_id"]
        event_name = event["event_name"]
        time = datetime.fromisoformat(event["date_iso8601"])
        event_uid = event["auth_user_uid"]
        if event_name == "Submission":
            last_submission_time_for_question[quid] = time
        elif event_name == "Score question" and uid != event_uid:
            # if question was scored by someone other than student, assume it was a staff member 
            # (could include a staff roster in the future)
            # fake the event time as last submission time
            time = last_submission_time_for_question[quid]
        event["date_iso8601"] = time # overwrite with datetime to avoid converting again later

    # now aggregate score questions as usual
    filtered_log = [
        (event["data"], event["date_iso8601"], event["question_id"])
        for event in filtered_log
        if event["event_name"] == "Score question"
    ]

    scores_per_question_orig = defaultdict(int)
    scores_per_question_makeup = defaultdict(int)

    for data, time, quid in filtered_log:
        # pick the best score per question before deadline and before makeup deadline
        if time <= due_date and data["points"] > scores_per_question_orig[quid]:
            scores_per_question_orig[quid] = data["points"]
        if time <= makeup_due_date and data["points"] > scores_per_question_makeup[quid]:
            scores_per_question_makeup[quid] = data["points"]

    orig = sum(scores_per_question_orig.values())
    makeup = sum(scores_per_question_makeup.values())
    # orig and makeup are the points before deadline and points before late deadline
    return orig, makeup


def fetch_grade(uid, assmt_instance_id, due_date, makeup_due_date):
    orig, makeup = get_grade(uid, assmt_instance_id, due_date, makeup_due_date)
    return {
        "uid": uid,
        "orig_points": orig,
        "orig_date": due_date,
        "makeup_points": makeup,
        "makeup_date": makeup_due_date,
    }


def main():
    assmt_id = get_assmt_id()
    due_date = get_assmt_due_date(assmt_id)
    makeup_due_date = due_date + timedelta(days=7, minutes=1)
    assmt_instances = get_assmt_instances(assmt_id)

    done_cnt = 0
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        future_to_uid = {
            executor.submit(fetch_grade, uid, assmt_instance_id, due_date, makeup_due_date): uid
            for uid, assmt_instance_id in assmt_instances.items()
        }

        for future in concurrent.futures.as_completed(future_to_uid):
            user_uid = future_to_uid[future]
            try:
                data = future.result()
                results.append(data)
            except Exception as e:
                logger.error(f"Error fetching grade for user {user_uid}: {e}")
            done_cnt += 1
            print(f"Progress: {done_cnt}/{len(assmt_instances)} ({done_cnt / len(assmt_instances) * 100:.2f}%)")

    results_filename = "total_score.csv"
    with open(f"{results_filename}", mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["uid", "instance", "points", "orig_points", "orig_date", "makeup_points", "makeup_date"])
        for item in results:
            if math.isclose(item["makeup_points"], item["orig_points"]):
                continue
            final_points = (item["orig_points"] + item["makeup_points"]) / 2
            writer.writerow(
                [
                    item["uid"],
                    1,
                    final_points,
                    item["orig_points"],
                    item["orig_date"],
                    item["makeup_points"],
                    item["makeup_date"],
                ]
            )

    print(f"Total score saved to {results_filename}")


if __name__ == "__main__":
    main()
