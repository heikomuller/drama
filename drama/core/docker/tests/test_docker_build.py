from pathlib import Path

import shutil
import tempfile
import unittest

from drama.core.docker.build import get_dockerfile


class CreateDockerfileTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # Create a temporary folder for the Dockerfile.
        self.tmpdir = tempfile.mkdtemp()

    def test_existing_dockerfile(self):
        """
        Test creating the Dockerfile from a repositpory that contains a
        Dockerfile.
        """
        # Default Docker file
        lines = ['A', 'B', 'C']
        with Path(self.tmpdir, "Dockerfile").open("wt") as f:
            for line in lines:
                f.write(f"{line}\n")
        dockerfile = get_dockerfile(
            spec={
                "tag": "test-image",
                "baseImage": "python:3.8",
                "requirements": ["package1", "package2"],
                "files": [{"src": "code/", "dst": "code/"}]
            },
            sourcedir=self.tmpdir,
            targetdir=self.tmpdir
        )
        self.assertEqual(
            dockerfile,
            lines
        )
        self.assertFalse(Path(self.tmpdir, "requirements.txt").is_file())
        # Explicit Dockerfile reference
        lines = ['X', 'Y', 'Z']
        with Path(self.tmpdir, "aDockerfile.txt").open("wt") as f:
            for line in lines:
                f.write(f"{line}\n")
        dockerfile = get_dockerfile(
            spec={
                "tag": "test-image",
                "baseImage": "python:3.8",
                "requirements": ["package1", "package2"],
                "files": [
                    {"src": "code/", "dst": "code/"},
                    {"src": "data/names.txt", "dst": "inputs/data.txt"}
                ],
                "dockerfile": 'aDockerfile.txt'
            },
            sourcedir=self.tmpdir,
            targetdir=self.tmpdir
        )
        self.assertEqual(
            dockerfile,
            lines
        )
        self.assertFalse(Path(self.tmpdir, "requirements.txt").is_file())

    def test_python_dockerfile(self):
        """
        Test creating the Dockerfile for a Python base image.
        """
        dockerfile = get_dockerfile(
            spec={
                "tag": "test-image",
                "baseImage": "python:3.8",
                "requirements": ["package1", "package2"],
                "files": [
                    {"src": "code/", "dst": "code/"},
                    {"src": "data/names.txt", "dst": "inputs/data.txt"}
                ]
            },
            sourcedir=self.tmpdir,
            targetdir=self.tmpdir
        )
        self.assertEqual(
            dockerfile,
            [
                "FROM python:3.8",
                "COPY code/ code/",
                "COPY data/names.txt inputs/data.txt",
                "COPY requirements.txt requirements.txt",
                "RUN pip install -r requirements.txt"
            ]
        )
        with Path(self.tmpdir, "requirements.txt").open("rt") as f:
            self.assertEqual(
                ["package1", "package2"],
                [line.strip() for line in f]
            )

    def test_r_dockerfile(self):
        """
        Test creating the Dockerfile for a R base image.
        """
        dockerfile = get_dockerfile(
            spec={
                "tag": "test-image",
                "baseImage": "rocker/r-ver",
                "packages": [
                    {"name": "package1", "dependencies": False, },
                    {"name": "package2", "repos": "repo-x"}
                ],
                "files": [
                    {"src": "mycode/", "dst": "code/"}
                ]
            },
            sourcedir=self.tmpdir,
            targetdir=self.tmpdir
        )
        self.assertEqual(
            dockerfile,
            [
                "FROM rocker/r-ver",
                "COPY mycode/ code/",
                "COPY install_packages.R install_packages.R",
                "RUN Rscript install_packages.R"
            ]
        )
        with Path(self.tmpdir, "install_packages.R").open("rt") as f:
            self.assertEqual(
                [
                    'install.packages("package1", dependencies=FALSE, repos="https://cran.r-project.org")',
                    'install.packages("package2", dependencies=TRUE, repos="repo-x")'
                ],
                [line.strip() for line in f]
            )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)


if __name__ == "__main__":
    unittest.main()
