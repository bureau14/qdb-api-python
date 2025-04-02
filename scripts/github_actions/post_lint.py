import logging
import os
import sys

from slack_sdk import WebClient

logger = logging.getLogger(__name__)
qdb_users = {
    "solatis": "U717WTKJ8",  # Leon
    "terngkub": "UAKV3RR55",  # Nattapol
    "igorniebylski": "U061GU3RHAR",  # Igor
    "rodp63": "U085W1KAGCW",  # Joaquin
}


def get_slack_data() -> dict[str, str]:
    return {
        "token": os.getenv("SLACK_BOT_TOKEN", ""),
        "channel": os.getenv("SLACK_BOT_CHANNEL", ""),
    }


def get_user(github_user: str) -> str:
    return (
        f"<@{qdb_users[github_user]}> ({github_user})"
        if qdb_users.get(github_user)
        else github_user
    )


def get_job_data() -> dict[str, str]:
    server = os.getenv("GITHUB_SERVER_URL")
    repository = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    actor = os.getenv("GITHUB_ACTOR")
    event_name = os.getenv("GITHUB_EVENT_NAME")
    ref = os.getenv("GITHUB_REF")
    workflow = os.getenv("GITHUB_WORKFLOW")
    sha = os.getenv("GITHUB_SHA")

    return {
        "url": f"{server}/{repository}/actions/runs/{run_id}",
        "repository": repository,
        "run_id": run_id,
        "actor": actor,
        "event_name": event_name,
        "ref": ref,
        "workflow": workflow,
        "sha": sha,
    }


def send_slack_message(client: WebClient, channel: str):
    job_data = get_job_data()
    client.chat_postMessage(
        channel=channel,
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "A workflow has failed :broken_heart:",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Repository* - {job_data['repository']}\n"
                        f"*Workflow* - {job_data['workflow']}\n"
                        f"*Triggered by* - {get_user(job_data['actor'])}\n"
                        f"*Event type* - {job_data['event_name']}\n"
                        f"*Event ref* - {job_data['ref']}\n"
                        f"*Commmit SHA* - {job_data['sha']}\n"
                        f"*Job link* - <{job_data['url']}|/actions/runs/{job_data['run_id']}>\n"
                    ),
                },
            },
        ],
    )


def main():
    job_exit_outcome = sys.argv[1]
    if job_exit_outcome == "failure":
        slack_data = get_slack_data()
        client = WebClient(token=slack_data["token"])
        send_slack_message(client, slack_data["channel"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
