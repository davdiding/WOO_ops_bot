import os

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

load_dotenv()

client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

try:
    response = client.chat_postMessage(channel="#general", text="Hello from your Python script!")
    assert response["message"]["text"] == "Hello from your Python script!"
except SlackApiError as e:
    assert e.response["ok"] is False
    assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
    print(f"Got an error: {e.response['error']}")
