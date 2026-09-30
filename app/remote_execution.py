"""Shared remote-execution errors used by orchestration layers."""


class SSHCommandError(RuntimeError):
    def __init__(self, message, output='', returncode=None):
        super().__init__(message)
        self.output = output
        self.returncode = returncode
