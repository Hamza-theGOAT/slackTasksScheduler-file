import json
import os
import asyncio
from slack_sdk import WebClient
from datetime import datetime as dt
from dotenv import load_dotenv


def getCredz():
    load_dotenv()
    userToken = os.getenv('userToken')
    client = WebClient(token=userToken)

    with open('tasks.json', 'r') as j:
        tasks = json.load(j)

    return client, tasks


async def slackMsg(client, channel, msg):
    response = client.chat_postMessage(
        channel=channel,
        text=msg
    )
    return response['ts']


async def scheduleTasks(client, tasks, task, schTime=None):
    pass


async def main():
    print("Starting task check...")
    client, tasks = getCredz()

    taskCorouts = []

    for task, config in tasks.items():
        hr = int(config['InitHour'])
        mn = int(config['InitMin'])

        schTime = f"{hr:02d}:{mn:02d}"

        # Add each task to our list of coroutines
        taskCorouts.append(
            schedulTasks(client, tasks, task, schTime)
        )
        print(f"Scheduled task '{task}' at {schTime}")

    # Run all tasks concurrently
    await asyncio.gather(*taskCorouts)


if __name__ == '__main__':
    asyncio.run(main())
