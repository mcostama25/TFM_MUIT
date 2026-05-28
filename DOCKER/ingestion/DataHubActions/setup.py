from setuptools import setup

setup(
    name="datahub-federation-forwarder",
    version="0.1.0",
    py_modules=["federation_forwarder"],
    install_requires=["acryl-datahub-actions", "requests"],
    entry_points={
        "datahub_actions.action.plugins": [
            "federation_forwarder = federation_forwarder:FederationForwardAction",
        ]
    },
)
