# streaming-05-smart-smoker

> Name: Jake Rood \
Date: May 29, 2024

Streaming data may come from web analytics, social media, smart devices, and more. In the next two modules, we'll look at implementing analytics for a "smart smoker" (as in slow cooked food). 

In this module, we'll understand the process, design our system, and implement the producer. In the next module, we'll add the consumers, implementing analytics based on a rolling window of time, and raise an alert when interesting events are detected.

## Prerequisites

1. Git
1. Python 3.7+ (3.11+ preferred)
1. VS Code Editor
1. RabbitMQ Server installed and running locally

## Before You Begin

1. Fork this starter repo into your GitHub.
1. Clone your repo down to your machine.

## Create a Python Virtual Environment

We will create a local Python virtual environment to isolate our project's third-party dependencies from other projects.

1. Open a terminal window in VS Code.
1. Use the built-in Python utility venv to create a new virtual environment named `.venv` in the current directory.

```shell
python3 -m venv .venv
```

Verify you get a new .venv directory in your project. 
We use .venv as the name to keep it away from our project files. 

## Activate the Virtual Environment

In the same VS Code terminal window, activate the virtual environment. On MacOS:

```shell
source .venv/bin/activate
```

Verify you see the virtual environment name (.venv) in your terminal prompt.

## Install Dependencies into the Virtual Environment

To work with RabbitMQ, we need to install the pika library.
A library is a collection of code that we can use in our own code.

We keep the list of third-party libraries needed in a file named requirements.txt.
Use the pip utility to install the libraries listed in requirements.txt into our active virtual environment. 

Make sure you can see the .venv name in your terminal prompt before running this command.

```shell
python3 -m pip install -r requirements.txt
```
