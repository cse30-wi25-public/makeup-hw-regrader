import requests
import logging
import time
import json
from urllib.parse import urljoin
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


def verify_time(date, title):
    prompt = f"{title} is {date}. Is this correct?\n" "(Enter 'Y' or a new time in the format [YYYY-MM-DD HH:MM:SS]):\n"
    print(prompt, end="")
    while True:
        user_input = input().strip()
        if user_input.upper() == "Y":
            return date
        else:
            try:
                new_time = datetime.strptime(user_input, "%Y-%m-%d %H:%M:%S")
                print(f"New due date: {new_time}")
                return new_time
            except ValueError:
                print("Invalid format. Please enter a valid date in the format [YYYY-MM-DD HH:MM:SS]")


class PLAPI:
    def __init__(self, domain, token, course_instance_id):
        self.token = token
        self.course_instance_id = course_instance_id
        self.server = urljoin(domain, "/pl/api/v1")
        self.logger = logging.getLogger("pl")

    def api_request(self, endpoint, retry_max=5):
        self.logger.info(f"start request to '{endpoint}'")
        url = self.server + endpoint
        retry_cnt = 0
        start_time = time.time()
        while True:
            response = requests.get(url, headers={"Private-Token": self.token})
            if response.status_code == 200:
                break
            elif response.status_code == 502:
                retry_cnt += 1
                if retry_cnt >= retry_max:
                    self.logger.error(f"Maximum number of retries reached on 502 Bad Gateway Error for {url}")
                    raise Exception(f"Maximum number of retries reached on 502 Bad Gateway Error for {url}")
                else:
                    self.logger.info(f"Bad Gateway Error encountered for {url}, retrying in 10 seconds")
                    time.sleep(10)
            else:
                self.logger.error(f"Invalid status returned for {url}: {response.status_code}")
                raise Exception(f"Invalid status returned for {url}: {response.status_code}")
        end_time = time.time()
        self.logger.info(
            f"request to '{endpoint}' {response.headers.get('content-length', -1)} bytes completed in {end_time - start_time:.2f} seconds"
        )

        return json.loads(response.text)

    def get_assmt(self):
        assmts = self.api_request(f"/course_instances/{self.course_instance_id}/assessments")
        name2id = dict()
        for assmt in assmts:
            id = assmt["assessment_id"]
            name = assmt["assessment_name"]
            name2id[str(name)] = id
            name2id[str(id)] = id
            print(f"id: {id}, name: {name}")
        while True:
            id = input("Enter the assessment (either name or id):\n")
            if id in name2id:
                break
            print("Invalid assessment name or id")
        self.logger.info(f"Selected assessment: {id}")

        return name2id[id]

    def get_assmt_instances(self, assmt_id):
        endpoint = f"/course_instances/{self.course_instance_id}/assessments/{assmt_id}/assessment_instances"
        instances = self.api_request(endpoint)
        uid2id = {item["user_uid"]: item["assessment_instance_id"] for item in instances}
        return uid2id

    def scan_hw_log(self, assmt_isntance_id, due_date, makeup_due_date):
        endpoint = (
            f"/course_instances/{self.course_instance_id}/assessment_instances/{assmt_isntance_id}/instance_questions"
        )
        questions = self.api_request(endpoint)

        total_points = sum(q["assessment_question_max_points"] for q in questions)

        manual_questions = [q["question_name"] for q in questions if q["assessment_question_max_manual_points"] > 0]

        logs = self.api_request(
            f"/course_instances/{self.course_instance_id}/assessment_instances/{assmt_isntance_id}/log"
        )
        logs.sort(key=lambda x: x["date_iso8601"])

        last_manual_score = {q: 0 for q in manual_questions}
        for event in logs:
            if event["event_name"] == "Manual grading results":
                last_manual_score[event["qid"]] = event["data"]["manual_points"]

        cur_total_score = 0
        cur_manual_score = {q: 0 for q in manual_questions}

        orig_score = None
        orig_date = None
        makeup_score = None
        makeup_date = None

        for event in logs:
            if event["event_name"] == "Score assessment":
                cur_total_score = event["data"]["points"]
            elif event["event_name"] == "Manual grading results":
                cur_manual_score[event["qid"]] = event["data"]["manual_points"]
            else:
                continue

            total_score = cur_total_score - sum(cur_manual_score.values()) + sum(last_manual_score.values())

            time = datetime.fromisoformat(event["date_iso8601"])
            if time <= due_date:
                if orig_score is None or total_score > orig_score:
                    orig_score = total_score
                    orig_date = time
            if time <= makeup_due_date:
                if makeup_score is None or total_score > makeup_score:
                    makeup_score = total_score
                    makeup_date = time

        return orig_score, orig_date, makeup_score, makeup_date, total_points

    def fetch_hw_grade(self, assmt_id):
        endpoint = f"/course_instances/{self.course_instance_id}/assessments/{assmt_id}/assessment_access_rules"
        access_rules = self.api_request(endpoint)
        due_date = max(
            [
                datetime.fromisoformat(t["end_date"])
                for t in access_rules
                if t["mode"] == "Public" and t["credit"] == 100
            ]
        )

        due_date = verify_time(due_date, "orig due date")
        makeup_due_date = verify_time(due_date + timedelta(days=7, minutes=1), "makeup due date")

        assmt_instances = self.get_assmt_instances(assmt_id)

        results = {}
        done_cnt = 0
        with ThreadPoolExecutor(max_workers=32) as executor:
            future_to_uid = {}
            for uid, instance_id in assmt_instances.items():
                future = executor.submit(self.scan_hw_log, instance_id, due_date, makeup_due_date)
                future_to_uid[future] = uid

            for future in as_completed(future_to_uid):
                uid = future_to_uid[future]
                done_cnt += 1
                print(f"Progress: {done_cnt}/{len(assmt_instances)} ({done_cnt / len(assmt_instances) * 100:.2f}%)")
                try:
                    orig_score, orig_date, makeup_score, makeup_date, total_points = future.result()
                except Exception as e:
                    self.logger.error(f"Error fetching grade for user {uid}: {e}")
                    continue
                if orig_score and makeup_score:
                    final_points = (orig_score + makeup_score) / 2
                elif orig_score:
                    final_points = orig_score
                elif makeup_score:
                    final_points = makeup_score / 2
                else:
                    final_points = 0

                results[uid] = {
                    "uid": uid,
                    "points": final_points,
                    "orig_points": orig_score,
                    "orig_date": orig_date,
                    "makeup_points": makeup_score,
                    "makeup_date": makeup_date,
                    "total_points": total_points,
                }

        return results

    def fetch_exam_grade(self, assmt_id):
        endpoint = f"/course_instances/{self.course_instance_id}/gradebook"
        gradebook = self.api_request(endpoint)
        results = {
            student["user_uid"]: {
                "uid": student["user_uid"],
                "score_perc": [q["score_perc"] for q in student["assessments"] if q["assessment_id"] == assmt_id][0],
            }
            for student in gradebook
        }
        return results

    def fetch_grade(self, assmt_id):
        endpoint = f"/course_instances/{self.course_instance_id}/assessments/{assmt_id}"
        assmt_info = self.api_request(endpoint)
        if assmt_info["type"] == "Homework":
            return self.fetch_hw_grade(assmt_id)
        elif assmt_info["type"] == "Exam":
            return self.fetch_exam_grade(assmt_id)
        else:
            raise Exception(f"Unknown assessment type: {assmt_info['type']}")
