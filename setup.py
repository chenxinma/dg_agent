from setuptools import setup, find_packages

setup(
    name="dg_agent",
    version="0.1.0",
    packages=find_packages(where="bot"),
    package_dir={"": "bot"},
    install_requires=[
        "requests>=2.31.0",
        "logfire>=0.1.0",
    ],
    entry_points={
        "console_scripts": [
            "bot=chat_app:main",
        ],
    },
    python_requires=">=3.9",
)
