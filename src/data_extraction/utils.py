SUPPORTED_LANGUAGES = [
    "python",
    "jupyter-notebook",
    "markdown",
    "html",
    "shell",
    "java",
    "javascript",
    "typescript",
    "c",
    "cpp",
    "csharp",
    "rust",
    "go",
]


def get_languages(value) -> list:
    output = value or SUPPORTED_LANGUAGES
    if isinstance(output, str):
        output = output.split(",")

    return output
