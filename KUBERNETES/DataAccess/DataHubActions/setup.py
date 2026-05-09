from setuptools import find_packages, setup

setup(
    name="event-logger-action",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    entry_points={
        "datahub_actions.action.plugins": [
            "event_logger = event_logger.action:EventLoggerAction",
        ],
    },
)
