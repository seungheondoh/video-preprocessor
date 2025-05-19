from setuptools import setup, find_packages

setup(
    name="video-preprocessor",
    version="0.1.0",
    packages=["vp"],
    install_requires=[
        "numpy",
        "pandas",
        "torch",
        "opencv-python",
        "boto3",  # For S3 interactions
        "requests",
        "tqdm",
        "pillow",
        "matplotlib",
        "scipy",
        "ffmpeg", # 안될 경우 "ffmpeg-python"을 사용하거나 sudo apt install ffmpeg로 설치.
        "yt-dlp",
        "torchlibrosa==0.1.0",
        "librosa",
        "julius",
    ],
    entry_points={
        "console_scripts": [
            "vp=vp.cli:main",
        ],
    },
    author="Seungheon Doh",
    author_email="seungheon.doh@gmail.com",
    description="A video preprocessing toolkit for machine learning applications",
    long_description=open("readme.md", "r").read() if open("readme.md", "r") else "",
    long_description_content_type="text/markdown",
    url="https://github.com/seungheondoh/video-preprocessor",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
